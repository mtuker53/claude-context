import json
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from claude_context.middleware.wsgi import ClaudeContextMiddleware


@pytest.fixture
def flask_app():
    app = Flask(__name__)

    @app.route("/api/orders", methods=["POST"])
    def create_order():
        return {"order_id": "123"}

    @app.route("/api/orders/<order_id>", methods=["GET"])
    def get_order(order_id):
        return {"order_id": order_id}

    return app


@pytest.fixture
def client(flask_app):
    with patch("claude_context.middleware.wsgi.make_flush_fn") as mock_make_flush:
        mock_make_flush.return_value = MagicMock()
        flask_app.wsgi_app = ClaudeContextMiddleware(
            flask_app.wsgi_app,
            service_name="test-api",
            table_name="test-table",
        )
        with flask_app.test_client() as c:
            yield c


class TestWSGIMiddleware:
    def test_passes_through_request(self, client):
        response = client.post(
            "/api/orders",
            data=json.dumps({"user_id": "1"}),
            content_type="application/json",
        )
        assert response.status_code == 200

    def test_does_not_consume_body(self, client):
        body = {"user_id": "abc", "cart_id": "xyz"}
        response = client.post(
            "/api/orders",
            data=json.dumps(body),
            content_type="application/json",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["order_id"] == "123"
