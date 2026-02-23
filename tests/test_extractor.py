import json

import pytest

from claude_context.capture.extractor import (
    build_route_template,
    extract_custom_headers,
    extract_fields,
    extract_fields_from_body,
    extract_query_params,
    normalize_path,
    resolve_caller,
)


class TestExtractFields:
    def test_top_level_keys(self):
        data = {"user_id": "123", "cart_id": "abc"}
        assert extract_fields(data, max_depth=1) == {"user_id", "cart_id"}

    def test_nested_dot_notation(self):
        data = {"user": {"id": "1", "name": "Alice"}}
        result = extract_fields(data, max_depth=2)
        assert "user" in result
        assert "user.id" in result
        assert "user.name" in result

    def test_array_bracket_notation(self):
        data = {"items": [{"sku": "A", "qty": 1}]}
        result = extract_fields(data, max_depth=2)
        assert "items" in result
        assert "items[].sku" in result
        assert "items[].qty" in result

    def test_max_depth_respected(self):
        data = {"a": {"b": {"c": "deep"}}}
        result = extract_fields(data, max_depth=2)
        assert "a" in result
        assert "a.b" in result
        assert "a.b.c" not in result

    def test_empty_dict(self):
        assert extract_fields({}) == set()


class TestExtractFieldsFromBody:
    def test_json_body(self):
        body = json.dumps({"user_id": "1", "amount": 50}).encode()
        result = extract_fields_from_body(body, "application/json")
        assert result == frozenset({"user_id", "amount"})

    def test_non_json_content_type(self):
        body = b"name=Alice&age=30"
        assert extract_fields_from_body(body, "application/x-www-form-urlencoded") == frozenset()

    def test_invalid_json(self):
        assert extract_fields_from_body(b"not json", "application/json") == frozenset()

    def test_empty_body(self):
        assert extract_fields_from_body(b"", "application/json") == frozenset()


class TestExtractCustomHeaders:
    def test_skips_standard_headers(self):
        headers = {"content-type": "application/json", "host": "example.com", "accept": "*/*"}
        assert extract_custom_headers(headers) == frozenset()

    def test_keeps_custom_headers(self):
        headers = {"x-service-name": "checkout", "x-correlation-id": "abc123"}
        result = extract_custom_headers(headers)
        assert "x-service-name" in result
        assert "x-correlation-id" in result

    def test_case_insensitive(self):
        headers = {"X-Service-Name": "checkout"}
        result = extract_custom_headers(headers)
        assert "x-service-name" in result


class TestExtractQueryParams:
    def test_single_param(self):
        assert extract_query_params("page=1") == frozenset({"page"})

    def test_multiple_params(self):
        result = extract_query_params("page=1&limit=20&sort=asc")
        assert result == frozenset({"page", "limit", "sort"})

    def test_empty_string(self):
        assert extract_query_params("") == frozenset()

    def test_param_names_only_no_values(self):
        result = extract_query_params("q=secret_value&filter=private")
        assert result == frozenset({"q", "filter"})


class TestNormalizePath:
    def test_numeric_id(self):
        assert normalize_path("/api/orders/123") == "/api/orders/{id}"

    def test_uuid(self):
        path = "/api/users/550e8400-e29b-41d4-a716-446655440000"
        assert normalize_path(path) == "/api/users/{uuid}"

    def test_no_ids(self):
        assert normalize_path("/api/orders") == "/api/orders"


class TestBuildRouteTemplate:
    def test_replaces_path_params(self):
        result = build_route_template("/api/orders/123", {"order_id": "123"})
        assert result == "/api/orders/{order_id}"

    def test_multiple_params(self):
        result = build_route_template("/api/orders/123/items/456", {"order_id": "123", "item_id": "456"})
        assert result == "/api/orders/{order_id}/items/{item_id}"


class TestResolveCaller:
    def test_x_service_name(self):
        assert resolve_caller({"x-service-name": "checkout"}) == "checkout"

    def test_x_caller_id(self):
        assert resolve_caller({"x-caller-id": "mobile-bff"}) == "mobile-bff"

    def test_fallback_user_agent(self):
        assert resolve_caller({"user-agent": "my-service/1.0"}) == "my-service"

    def test_unknown_fallback(self):
        assert resolve_caller({}) == "unknown"

    def test_priority_order(self):
        headers = {"x-service-name": "checkout", "x-caller-id": "other", "user-agent": "ua"}
        assert resolve_caller(headers) == "checkout"
