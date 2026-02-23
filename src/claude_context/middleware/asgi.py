import logging
from collections.abc import Callable
from datetime import datetime, timezone

from claude_context.capture.buffer import ObservationBuffer
from claude_context.capture.extractor import (
    build_route_template,
    extract_custom_headers,
    extract_fields_from_body,
    extract_query_params,
    normalize_path,
    resolve_caller,
)
from claude_context.capture.observation import Observation
from claude_context.storage.dynamo import make_flush_fn

logger = logging.getLogger(__name__)


class ClaudeContextMiddleware:
    """ASGI middleware for FastAPI and Starlette applications."""

    def __init__(
        self,
        app,
        *,
        service_name: str,
        table_name: str = "claude-context",
        region: str | None = None,
        max_body_depth: int = 3,
        caller_resolver: Callable[[dict[str, str]], str] | None = None,
        buffer_max_size: int = 100,
        buffer_flush_interval: float = 30.0,
        ttl_days: int = 90,
    ) -> None:
        self.app = app
        self.service_name = service_name
        self.max_body_depth = max_body_depth
        self.caller_resolver = caller_resolver or resolve_caller

        flush_fn = make_flush_fn(table_name=table_name, region=region, ttl_days=ttl_days)
        self._buffer = ObservationBuffer(
            flush_fn=flush_fn,
            max_size=buffer_max_size,
            flush_interval=buffer_flush_interval,
        )

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        body_chunks: list[bytes] = []

        async def capturing_receive():
            message = await receive()
            if message["type"] == "http.request":
                body_chunks.append(message.get("body", b""))
            return message

        status_code: list[int] = [200]

        async def capturing_send(message):
            if message["type"] == "http.response.start":
                status_code[0] = message["status"]
            await send(message)

        await self.app(scope, capturing_receive, capturing_send)

        try:
            self._record(scope, b"".join(body_chunks), status_code[0])
        except Exception:
            logger.warning("claude-context: failed to record observation", exc_info=True)

    def _record(self, scope: dict, body: bytes, status_code: int) -> None:
        headers = {
            k.decode(): v.decode()
            for k, v in scope.get("headers", [])
        }
        content_type = headers.get("content-type", "")
        query_string = scope.get("query_string", b"").decode()
        path = scope.get("path", "/")
        path_params: dict = scope.get("path_params", {})

        if path_params:
            path_template = build_route_template(path, {k: str(v) for k, v in path_params.items()})
        else:
            path_template = normalize_path(path)

        obs = Observation(
            service_name=self.service_name,
            caller=self.caller_resolver(headers),
            method=scope.get("method", "GET").upper(),
            path_template=path_template,
            request_fields=extract_fields_from_body(body, content_type, self.max_body_depth),
            request_headers=extract_custom_headers(headers),
            query_params=extract_query_params(query_string),
            status_code=status_code,
            timestamp=datetime.now(timezone.utc),
        )
        self._buffer.add(obs)
