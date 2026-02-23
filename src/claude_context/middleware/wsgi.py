import io
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

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


class ClaudeContextMiddleware:
    """WSGI middleware for Flask and Django applications."""

    def __init__(
        self,
        wsgi_app,
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
        self.wsgi_app = wsgi_app
        self.service_name = service_name
        self.max_body_depth = max_body_depth
        self.caller_resolver = caller_resolver or resolve_caller

        flush_fn = make_flush_fn(table_name=table_name, region=region, ttl_days=ttl_days)
        self._buffer = ObservationBuffer(
            flush_fn=flush_fn,
            max_size=buffer_max_size,
            flush_interval=buffer_flush_interval,
        )

    def __call__(self, environ: dict, start_response):
        content_length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(content_length)
        environ["wsgi.input"] = io.BytesIO(body)

        status_code: list[int] = [200]

        def capturing_start_response(status: str, headers, exc_info=None):
            try:
                status_code[0] = int(status.split(" ", 1)[0])
            except (ValueError, IndexError):
                pass
            return start_response(status, headers, exc_info)

        response = self.wsgi_app(environ, capturing_start_response)

        try:
            self._record(environ, body, status_code[0])
        except Exception:
            logger.warning("claude-context: failed to record observation", exc_info=True)

        return response

    def _record(self, environ: dict, body: bytes, status_code: int) -> None:
        headers = self._extract_headers(environ)
        content_type = environ.get("CONTENT_TYPE", "")
        query_string = environ.get("QUERY_STRING", "")
        method = environ.get("REQUEST_METHOD", "GET").upper()

        # Flask sets PATH_INFO; prefer url_rule template if available via thread-local
        path = environ.get("PATH_INFO", "/")
        path_template = self._get_flask_route(environ) or normalize_path(path)

        obs = Observation(
            service_name=self.service_name,
            caller=self.caller_resolver(headers),
            method=method,
            path_template=path_template,
            request_fields=extract_fields_from_body(body, content_type, self.max_body_depth),
            request_headers=extract_custom_headers(headers),
            query_params=extract_query_params(query_string),
            status_code=status_code,
            timestamp=datetime.now(timezone.utc),
        )
        self._buffer.add(obs)

    @staticmethod
    def _extract_headers(environ: dict) -> dict[str, str]:
        headers: dict[str, str] = {}
        for key, value in environ.items():
            if key.startswith("HTTP_"):
                header_name = key[5:].replace("_", "-").lower()
                headers[header_name] = value
            elif key == "CONTENT_TYPE":
                headers["content-type"] = value
            elif key == "CONTENT_LENGTH":
                headers["content-length"] = value
        return headers

    @staticmethod
    def _get_flask_route(environ: dict) -> str | None:
        """Extract route template from Flask's thread-local request if available."""
        try:
            from flask import request as flask_request
            rule = flask_request.url_rule
            if rule is not None:
                return str(rule)
        except Exception:
            pass
        return None
