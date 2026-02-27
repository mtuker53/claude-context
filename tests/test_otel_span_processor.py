from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import SpanContext, SpanKind, TraceFlags

from claude_context.otel.span_processor import (
    ClaudeContextSpanProcessor,
    default_span_caller_resolver,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TRACE_ID = 0x000000000000000000000000DEADBEEF
_SPAN_ID = 0x00000000DEADBEF0
_END_TIME_NS = int(datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9)


def _make_span(
    kind: SpanKind = SpanKind.SERVER,
    attributes: dict | None = None,
    end_time: int = _END_TIME_NS,
) -> ReadableSpan:
    """Build a minimal ReadableSpan for testing."""
    ctx = SpanContext(
        trace_id=_TRACE_ID,
        span_id=_SPAN_ID,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return ReadableSpan(
        name="test-span",
        context=ctx,
        kind=kind,
        attributes=attributes or {},
        end_time=end_time,
        instrumentation_scope=InstrumentationScope("test"),
    )


def _make_processor(**kwargs) -> ClaudeContextSpanProcessor:
    with patch("claude_context.otel.span_processor.make_flush_fn", return_value=MagicMock()):
        return ClaudeContextSpanProcessor(service_name="my-api", **kwargs)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

class TestSpanFiltering:
    def test_skips_client_spans(self):
        proc = _make_processor()
        span = _make_span(kind=SpanKind.CLIENT, attributes={"http.method": "GET"})
        proc._buffer = MagicMock()
        proc.on_end(span)
        proc._buffer.add.assert_not_called()

    def test_skips_internal_spans(self):
        proc = _make_processor()
        span = _make_span(kind=SpanKind.INTERNAL, attributes={"http.method": "GET"})
        proc._buffer = MagicMock()
        proc.on_end(span)
        proc._buffer.add.assert_not_called()

    def test_skips_non_http_server_spans(self):
        proc = _make_processor()
        span = _make_span(kind=SpanKind.SERVER, attributes={"db.system": "postgresql"})
        proc._buffer = MagicMock()
        proc.on_end(span)
        proc._buffer.add.assert_not_called()

    def test_processes_http_server_spans(self):
        proc = _make_processor()
        span = _make_span(kind=SpanKind.SERVER, attributes={"http.method": "GET", "http.route": "/api/orders"})
        proc._buffer = MagicMock()
        proc.on_end(span)
        proc._buffer.add.assert_called_once()


# ---------------------------------------------------------------------------
# Semantic convention handling — old (v1.x)
# ---------------------------------------------------------------------------

class TestOldSemconv:
    def setup_method(self):
        self.proc = _make_processor()
        self.proc._buffer = MagicMock()

    def test_extracts_method(self):
        span = _make_span(attributes={"http.method": "POST", "http.route": "/api/orders"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.method == "POST"

    def test_extracts_status_code(self):
        span = _make_span(attributes={"http.method": "GET", "http.route": "/", "http.status_code": 404})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.status_code == 404

    def test_extracts_query_from_http_target(self):
        span = _make_span(attributes={
            "http.method": "GET",
            "http.route": "/api/orders",
            "http.target": "/api/orders?page=1&limit=20",
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert "page" in obs.query_params
        assert "limit" in obs.query_params

    def test_uses_http_route_as_template(self):
        span = _make_span(attributes={
            "http.method": "GET",
            "http.route": "/api/orders/{order_id}",
            "http.target": "/api/orders/123",
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.path_template == "/api/orders/{order_id}"


# ---------------------------------------------------------------------------
# Semantic convention handling — new (v1.21+)
# ---------------------------------------------------------------------------

class TestNewSemconv:
    def setup_method(self):
        self.proc = _make_processor()
        self.proc._buffer = MagicMock()

    def test_extracts_method(self):
        span = _make_span(attributes={"http.request.method": "PUT", "http.route": "/api/orders/{id}"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.method == "PUT"

    def test_extracts_status_code(self):
        span = _make_span(attributes={
            "http.request.method": "DELETE",
            "http.route": "/api/orders/{id}",
            "http.response.status_code": 204,
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.status_code == 204

    def test_extracts_query_from_url_query(self):
        span = _make_span(attributes={
            "http.request.method": "GET",
            "http.route": "/api/orders",
            "url.query": "sort=asc&filter=active",
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert "sort" in obs.query_params
        assert "filter" in obs.query_params

    def test_new_semconv_takes_priority_over_old(self):
        span = _make_span(attributes={
            "http.request.method": "PATCH",   # new
            "http.method": "GET",             # old — should be ignored
            "http.route": "/api/orders/{id}",
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.method == "PATCH"


# ---------------------------------------------------------------------------
# Route template / path normalization
# ---------------------------------------------------------------------------

class TestPathTemplate:
    def setup_method(self):
        self.proc = _make_processor()
        self.proc._buffer = MagicMock()

    def test_uses_http_route_when_present(self):
        span = _make_span(attributes={"http.method": "GET", "http.route": "/api/users/{user_id}"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.path_template == "/api/users/{user_id}"

    def test_normalizes_numeric_id_when_no_route(self):
        span = _make_span(attributes={"http.method": "GET", "url.path": "/api/orders/123"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.path_template == "/api/orders/{id}"

    def test_normalizes_uuid_when_no_route(self):
        span = _make_span(attributes={
            "http.method": "GET",
            "url.path": "/api/users/550e8400-e29b-41d4-a716-446655440000",
        })
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.path_template == "/api/users/{uuid}"


# ---------------------------------------------------------------------------
# Caller resolution
# ---------------------------------------------------------------------------

class TestCallerResolution:
    def test_resolves_from_x_service_name_header(self):
        attrs = {"http.request.header.x_service_name": "checkout-service"}
        assert default_span_caller_resolver(attrs) == "checkout-service"

    def test_resolves_from_x_caller_id_header(self):
        attrs = {"http.request.header.x_caller_id": "mobile-bff"}
        assert default_span_caller_resolver(attrs) == "mobile-bff"

    def test_resolves_from_sequence_value(self):
        # OTEL stores captured headers as sequences
        attrs = {"http.request.header.x_service_name": ["checkout-service"]}
        assert default_span_caller_resolver(attrs) == "checkout-service"

    def test_falls_back_to_user_agent(self):
        attrs = {"http.user_agent": "my-service/1.2.3"}
        assert default_span_caller_resolver(attrs) == "my-service"

    def test_unknown_when_no_signal(self):
        assert default_span_caller_resolver({}) == "unknown"

    def test_custom_resolver_used(self):
        custom = lambda attrs: "hardcoded-caller"
        proc = _make_processor(caller_resolver=custom)
        proc._buffer = MagicMock()
        span = _make_span(attributes={"http.method": "GET", "http.route": "/"})
        proc.on_end(span)
        obs = proc._buffer.add.call_args[0][0]
        assert obs.caller == "hardcoded-caller"


# ---------------------------------------------------------------------------
# Observation fields
# ---------------------------------------------------------------------------

class TestObservationFields:
    def setup_method(self):
        self.proc = _make_processor()
        self.proc._buffer = MagicMock()

    def test_service_name_set(self):
        span = _make_span(attributes={"http.method": "GET", "http.route": "/"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.service_name == "my-api"

    def test_request_fields_empty(self):
        # OTEL spans don't contain body content — always empty
        span = _make_span(attributes={"http.method": "POST", "http.route": "/api/orders"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.request_fields == frozenset()

    def test_request_headers_empty(self):
        span = _make_span(attributes={"http.method": "GET", "http.route": "/"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        assert obs.request_headers == frozenset()

    def test_timestamp_derived_from_span_end_time(self):
        span = _make_span(attributes={"http.method": "GET", "http.route": "/"})
        self.proc.on_end(span)
        obs = self.proc._buffer.add.call_args[0][0]
        expected = datetime(2026, 2, 21, 12, 0, 0, tzinfo=timezone.utc)
        assert obs.timestamp == expected


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_shutdown_flushes_buffer(self):
        proc = _make_processor()
        proc._buffer = MagicMock()
        proc.shutdown()
        proc._buffer.flush.assert_called_once()

    def test_force_flush_flushes_buffer(self):
        proc = _make_processor()
        proc._buffer = MagicMock()
        result = proc.force_flush()
        proc._buffer.flush.assert_called_once()
        assert result is True

    def test_on_end_exception_does_not_propagate(self):
        proc = _make_processor()
        proc._buffer = MagicMock(side_effect=Exception("boom"))
        span = _make_span(attributes={"http.method": "GET", "http.route": "/"})
        # Should not raise
        proc.on_end(span)
