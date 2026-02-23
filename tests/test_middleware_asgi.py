import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from claude_context.middleware.asgi import ClaudeContextMiddleware


@pytest.fixture
def app():
    _app = FastAPI()

    @_app.post("/api/orders")
    def create_order():
        return {"order_id": "123"}

    @_app.get("/api/orders/{order_id}")
    def get_order(order_id: str):
        return {"order_id": order_id}

    return _app


@pytest.fixture
def client(app):
    with patch("claude_context.middleware.asgi.make_flush_fn") as mock_make_flush:
        mock_make_flush.return_value = MagicMock()
        app.add_middleware(
            ClaudeContextMiddleware,
            service_name="test-api",
            table_name="test-table",
        )
        with TestClient(app) as c:
            yield c


class TestASGIMiddleware:
    def test_passes_through_request(self, client):
        response = client.post("/api/orders", json={"user_id": "1"})
        assert response.status_code == 200

    def test_does_not_consume_body(self, client):
        body = {"user_id": "abc", "cart_id": "xyz"}
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200

    def test_non_http_scopes_pass_through(self, app):
        """WebSocket and lifespan scopes should not be intercepted."""
        with patch("claude_context.middleware.asgi.make_flush_fn") as mock_make_flush:
            mock_flush = MagicMock()
            mock_make_flush.return_value = mock_flush
            app.add_middleware(
                ClaudeContextMiddleware,
                service_name="test-api",
                table_name="test-table",
            )
            with TestClient(app) as client:
                # Regular HTTP should work
                response = client.get("/api/orders/123")
                assert response.status_code == 200
