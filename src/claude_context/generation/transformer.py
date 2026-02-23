def transform_items(items: list[dict]) -> dict[str, list[dict]]:
    """
    Convert raw DynamoDB items into a dict keyed by endpoint ("METHOD /path"),
    with each value being a list of caller dicts sorted by call_count descending.
    """
    endpoints: dict[str, list[dict]] = {}

    for item in items:
        # SK format: "CALLER#{caller}#{method}#{path_template}"
        parts = item["SK"].split("#", 3)
        if len(parts) != 4:
            continue
        _, caller, method, path = parts
        endpoint_key = f"{method} {path}"

        endpoints.setdefault(endpoint_key, []).append({
            "caller":          caller,
            "request_fields":  sorted(item.get("request_fields", [])),
            "request_headers": sorted(item.get("request_headers", [])),
            "query_params":    sorted(item.get("query_params", [])),
            "response_codes":  sorted(item.get("response_codes", [])),
            "call_count":      int(item.get("call_count", 0)),
            "last_seen":       str(item.get("last_seen", "")),
        })

    # Sort endpoints alphabetically and callers by call_count descending
    # — produces stable output so CLAUDE.md diffs are clean
    return {
        endpoint: sorted(callers, key=lambda c: -c["call_count"])
        for endpoint, callers in sorted(endpoints.items())
    }
