import json
import re

# Standard HTTP headers that carry no useful API consumer signal
_SKIP_HEADERS = frozenset({
    "host", "content-type", "content-length", "transfer-encoding",
    "accept", "accept-encoding", "accept-language", "accept-charset",
    "connection", "keep-alive", "upgrade-insecure-requests",
    "authorization", "cookie", "set-cookie",
    "cache-control", "pragma", "expires",
    "origin", "referer", "user-agent",
    "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "sec-fetch-user",
    "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
})

# Fallback path normalization patterns (applied when framework route template unavailable)
_PATH_PATTERNS = [
    (re.compile(r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "/{uuid}"),
    (re.compile(r"/\d+"), "/{id}"),
]


def extract_fields(data: dict, max_depth: int = 3, _depth: int = 1, _prefix: str = "") -> set[str]:
    """Recursively extract field names from a JSON object using dot notation."""
    fields: set[str] = set()
    for key, value in data.items():
        field_name = f"{_prefix}{key}"
        fields.add(field_name)
        if _depth < max_depth:
            if isinstance(value, dict):
                fields |= extract_fields(value, max_depth, _depth + 1, f"{field_name}.")
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        fields |= extract_fields(item, max_depth, _depth + 1, f"{field_name}[].")
    return fields


def extract_fields_from_body(body: bytes, content_type: str, max_depth: int = 3) -> frozenset[str]:
    """Parse a request body and return observed field names."""
    if not body or "application/json" not in content_type:
        return frozenset()
    try:
        data = json.loads(body)
        if isinstance(data, dict):
            return frozenset(extract_fields(data, max_depth))
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    return frozenset()


def extract_custom_headers(headers: dict[str, str]) -> frozenset[str]:
    """Return header names that are non-standard and carry API consumer signal."""
    return frozenset(
        name.lower()
        for name in headers
        if name.lower() not in _SKIP_HEADERS
    )


def extract_query_params(query_string: str) -> frozenset[str]:
    """Return query parameter names from a query string."""
    if not query_string:
        return frozenset()
    params: set[str] = set()
    for part in query_string.split("&"):
        key = part.split("=", 1)[0]
        if key:
            params.add(key)
    return frozenset(params)


def normalize_path(path: str) -> str:
    """Fallback path normalization when framework route template is unavailable."""
    for pattern, replacement in _PATH_PATTERNS:
        path = pattern.sub(replacement, path)
    return path


def build_route_template(path: str, path_params: dict[str, str]) -> str:
    """Reconstruct route template from actual path + matched path parameters."""
    template = path
    for name, value in path_params.items():
        template = template.replace(str(value), f"{{{name}}}")
    return template


def resolve_caller(headers: dict[str, str]) -> str:
    """Default caller identity resolution from request headers."""
    lowered = {k.lower(): v for k, v in headers.items()}
    for header in ("x-service-name", "x-caller-id", "x-source-service"):
        if header in lowered and lowered[header].strip():
            return lowered[header].strip()
    user_agent = lowered.get("user-agent", "")
    if user_agent:
        return user_agent.split("/")[0].strip() or "unknown"
    return "unknown"
