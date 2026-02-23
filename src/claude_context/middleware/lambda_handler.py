import functools
import logging
from datetime import datetime, timezone
from typing import Any

from claude_context.capture.buffer import ObservationBuffer
from claude_context.capture.extractor import (
    extract_custom_headers,
    extract_fields_from_body,
    extract_query_params,
    normalize_path,
    resolve_caller,
)
from claude_context.capture.observation import Observation
from claude_context.storage.dynamo import make_flush_fn

logger = logging.getLogger(__name__)

_TRIGGER_APIGW_V1 = "apigw_v1"
_TRIGGER_APIGW_V2 = "apigw_v2"
_TRIGGER_ALB = "alb"
_TRIGGER_AUTO = "auto"


def _detect_trigger(event: dict) -> str | None:
    """Return the detected Lambda trigger type, or None if not an HTTP event."""
    if "requestContext" in event and "http" in event.get("requestContext", {}):
        return _TRIGGER_APIGW_V2
    if "httpMethod" in event and "requestContext" in event:
        return _TRIGGER_APIGW_V1
    if "httpMethod" in event and "requestContext" not in event:
        return _TRIGGER_ALB
    return None


def _parse_event(event: dict, trigger: str) -> dict[str, Any] | None:
    """Extract HTTP context from a Lambda event. Returns None if not parseable."""
    try:
        if trigger == _TRIGGER_APIGW_V2:
            http = event["requestContext"]["http"]
            return {
                "method": http.get("method", "GET").upper(),
                "path": http.get("path", "/"),
                "headers": event.get("headers") or {},
                "query_string": event.get("rawQueryString", ""),
                "body": (event.get("body") or "").encode(),
            }
        if trigger in (_TRIGGER_APIGW_V1, _TRIGGER_ALB):
            headers = event.get("headers") or event.get("multiValueHeaders") or {}
            # multiValueHeaders values are lists — flatten to last value
            if headers and isinstance(next(iter(headers.values()), None), list):
                headers = {k: v[-1] for k, v in headers.items() if v}
            params = event.get("queryStringParameters") or {}
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            return {
                "method": event.get("httpMethod", "GET").upper(),
                "path": event.get("path", "/"),
                "headers": headers,
                "query_string": query_string,
                "body": (event.get("body") or "").encode(),
            }
    except (KeyError, AttributeError, TypeError):
        pass
    return None


def _get_status_code(result: Any) -> int:
    if isinstance(result, dict):
        return int(result.get("statusCode", 200))
    return 200


def claude_context_tracker(
    service_name: str,
    *,
    table_name: str = "claude-context",
    region: str | None = None,
    max_body_depth: int = 3,
    caller_resolver=None,
    trigger: str = _TRIGGER_AUTO,
    ttl_days: int = 90,
):
    """Decorator that records HTTP observations from Lambda invocations."""
    _caller_resolver = caller_resolver or resolve_caller
    flush_fn = make_flush_fn(table_name=table_name, region=region, ttl_days=ttl_days)
    buffer = ObservationBuffer(flush_fn=flush_fn, max_size=100, flush_interval=30.0)

    def decorator(handler):
        @functools.wraps(handler)
        def wrapper(event: dict, context):
            result = handler(event, context)

            try:
                resolved_trigger = trigger if trigger != _TRIGGER_AUTO else _detect_trigger(event)
                if resolved_trigger:
                    parsed = _parse_event(event, resolved_trigger)
                    if parsed:
                        headers: dict[str, str] = {
                            k.lower(): v for k, v in (parsed["headers"] or {}).items()
                        }
                        content_type = headers.get("content-type", "")
                        obs = Observation(
                            service_name=service_name,
                            caller=_caller_resolver(headers),
                            method=parsed["method"],
                            path_template=normalize_path(parsed["path"]),
                            request_fields=extract_fields_from_body(
                                parsed["body"], content_type, max_body_depth
                            ),
                            request_headers=extract_custom_headers(headers),
                            query_params=extract_query_params(parsed["query_string"]),
                            status_code=_get_status_code(result),
                            timestamp=datetime.now(timezone.utc),
                        )
                        buffer.add(obs)
            except Exception:
                logger.warning("claude-context: failed to record Lambda observation", exc_info=True)
            finally:
                # Always flush synchronously before Lambda freezes the execution environment
                buffer.flush()

            return result

        return wrapper

    return decorator
