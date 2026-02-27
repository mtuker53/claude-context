import logging
from collections.abc import Callable
from datetime import datetime, timezone

from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor
from opentelemetry.trace import SpanKind

from claude_context.capture.buffer import ObservationBuffer
from claude_context.capture.extractor import extract_query_params, normalize_path
from claude_context.capture.observation import Observation
from claude_context.storage.dynamo import make_flush_fn

logger = logging.getLogger(__name__)

# Semantic convention keys — support both old (v1.x) and new (v1.21+) conventions
_METHOD_KEYS = ("http.request.method", "http.method")
_STATUS_KEYS = ("http.response.status_code", "http.status_code")
_QUERY_KEYS = ("url.query",)
_PATH_KEYS = ("url.path",)
_TARGET_KEY = "http.target"
_ROUTE_KEY = "http.route"
_USER_AGENT_KEY = "http.user_agent"

# OTEL captures request headers as "http.request.header.{name}" (lowercased, hyphens→underscores)
_CALLER_HEADER_KEYS = (
    "http.request.header.x_service_name",
    "http.request.header.x_caller_id",
    "http.request.header.x_source_service",
)


def default_span_caller_resolver(attributes: dict) -> str:
    """
    Resolve caller identity from OTEL span attributes.

    Checks for service identity headers captured by OTEL instrumentation
    (requires OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST to include them),
    then falls back to User-Agent.
    """
    for key in _CALLER_HEADER_KEYS:
        value = attributes.get(key)
        if value:
            # OTEL stores captured header values as sequences
            if isinstance(value, (list, tuple)):
                value = value[0]
            if str(value).strip():
                return str(value).strip()

    user_agent = attributes.get(_USER_AGENT_KEY, "")
    if user_agent:
        return str(user_agent).split("/")[0].strip() or "unknown"

    return "unknown"


def _extract_query_string(attributes: dict) -> str:
    """Extract query string from span attributes, handling both semconv versions."""
    # New semconv: url.query is just the query string
    query = attributes.get("url.query")
    if query:
        return str(query)

    # Old semconv: http.target is the full path+query (e.g. "/api/orders?page=1")
    target = attributes.get(_TARGET_KEY, "")
    if "?" in target:
        return target.split("?", 1)[1]

    return ""


def _extract_path(attributes: dict) -> str:
    """Extract raw path from span attributes, handling both semconv versions."""
    path = attributes.get("url.path")
    if path:
        return str(path)

    target = attributes.get(_TARGET_KEY, "")
    return target.split("?", 1)[0] if target else "/"


def _get_attr(attributes: dict, *keys: str) -> str | None:
    """Return the first non-empty value found among the given attribute keys."""
    for key in keys:
        value = attributes.get(key)
        if value is not None:
            return str(value)
    return None


class ClaudeContextSpanProcessor(SpanProcessor):
    """
    OpenTelemetry SpanProcessor that records API consumer patterns into DynamoDB.

    Use this instead of the HTTP middleware when your service already has
    OpenTelemetry instrumentation in place.

    Usage::

        from opentelemetry.sdk.trace import TracerProvider
        from claude_context.otel import ClaudeContextSpanProcessor

        provider = TracerProvider()
        provider.add_span_processor(
            ClaudeContextSpanProcessor(service_name="my-api")
        )

    For reliable caller identity, configure OTEL to capture service identity headers::

        OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST=x-service-name
    """

    def __init__(
        self,
        service_name: str,
        *,
        table_name: str = "claude-context",
        region: str | None = None,
        caller_resolver: Callable[[dict], str] | None = None,
        buffer_max_size: int = 100,
        buffer_flush_interval: float = 30.0,
        ttl_days: int = 90,
    ) -> None:
        self.service_name = service_name
        self._caller_resolver = caller_resolver or default_span_caller_resolver

        flush_fn = make_flush_fn(table_name=table_name, region=region, ttl_days=ttl_days)
        self._buffer = ObservationBuffer(
            flush_fn=flush_fn,
            max_size=buffer_max_size,
            flush_interval=buffer_flush_interval,
        )

    def on_start(self, span, parent_context=None) -> None:
        pass  # Nothing to do at span start

    def on_end(self, span: ReadableSpan) -> None:
        try:
            self._process(span)
        except Exception:
            logger.warning("claude-context: failed to process span", exc_info=True)

    def shutdown(self) -> None:
        self._buffer.flush()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        self._buffer.flush()
        return True

    def _process(self, span: ReadableSpan) -> None:
        # Only process server-side spans
        if span.kind != SpanKind.SERVER:
            return

        attributes = dict(span.attributes or {})

        # Only process HTTP spans — must have a method attribute
        method = _get_attr(attributes, *_METHOD_KEYS)
        if not method:
            return

        # Route template: http.route is the same in both semconv versions
        # and is already normalized (e.g. "/api/orders/{order_id}")
        route = attributes.get(_ROUTE_KEY)
        if route:
            path_template = str(route)
        else:
            path_template = normalize_path(_extract_path(attributes))

        status_code_raw = _get_attr(attributes, *_STATUS_KEYS)
        status_code = int(status_code_raw) if status_code_raw else 0

        query_string = _extract_query_string(attributes)

        obs = Observation(
            service_name=self.service_name,
            caller=self._caller_resolver(attributes),
            method=method.upper(),
            path_template=path_template,
            request_fields=frozenset(),   # Not available in OTEL spans
            request_headers=frozenset(),  # Only if explicitly captured via OTEL config
            query_params=extract_query_params(query_string),
            status_code=status_code,
            timestamp=datetime.fromtimestamp(span.end_time / 1e9, tz=timezone.utc),
        )
        self._buffer.add(obs)
