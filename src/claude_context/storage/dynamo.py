import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

from claude_context.capture.aggregator import aggregate
from claude_context.capture.observation import AggregatedObservation, Observation

logger = logging.getLogger(__name__)


def write_observation(table, agg: AggregatedObservation, ttl_days: int = 90) -> None:
    """Write a single aggregated observation to DynamoDB using atomic ADD/SET operations."""
    pk = f"SERVICE#{agg.service_name}"
    sk = f"CALLER#{agg.caller}#{agg.method}#{agg.path_template}"
    ttl = int((agg.last_seen + timedelta(days=ttl_days)).timestamp())

    # DynamoDB requires each clause keyword (SET, ADD) to appear only once.
    # Collect all SET and ADD expressions separately then join them.
    set_parts = [
        "last_seen = :last_seen",
        "first_seen = if_not_exists(first_seen, :first_seen)",
        "#ttl = :ttl",
    ]
    add_parts = ["call_count :count"]
    attr_values: dict = {
        ":count": Decimal(agg.call_count),
        ":last_seen": agg.last_seen.isoformat(),
        ":first_seen": agg.first_seen.isoformat(),
        ":ttl": ttl,
    }

    # DynamoDB rejects ADD on an empty String Set — guard each one
    if agg.request_fields:
        add_parts.append("request_fields :rf")
        attr_values[":rf"] = agg.request_fields
    if agg.request_headers:
        add_parts.append("request_headers :rh")
        attr_values[":rh"] = agg.request_headers
    if agg.query_params:
        add_parts.append("query_params :qp")
        attr_values[":qp"] = agg.query_params
    if agg.response_codes:
        add_parts.append("response_codes :rc")
        attr_values[":rc"] = agg.response_codes

    update_expression = f"SET {', '.join(set_parts)} ADD {', '.join(add_parts)}"

    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=update_expression,
        ExpressionAttributeNames={
            "#ttl": "ttl",   # ttl is a reserved word in DynamoDB
        },
        ExpressionAttributeValues=attr_values,
    )


def flush_observations(
    observations: list[Observation],
    table,
    ttl_days: int = 90,
) -> None:
    """Aggregate a batch of observations and write them to DynamoDB in parallel."""
    aggregated = aggregate(observations)
    if not aggregated:
        return

    with ThreadPoolExecutor(max_workers=min(len(aggregated), 10)) as executor:
        futures = [
            executor.submit(write_observation, table, agg, ttl_days)
            for agg in aggregated
        ]
        for future in futures:
            try:
                future.result()
            except Exception:
                logger.warning("claude-context: DynamoDB write failed", exc_info=True)


def make_flush_fn(
    table_name: str,
    region: str | None,
    ttl_days: int,
) -> Callable[[list[Observation]], None]:
    """Return a flush function that writes to a specific DynamoDB table."""
    _table = None

    def get_table():
        nonlocal _table
        if _table is None:
            dynamodb = boto3.resource("dynamodb", region_name=region)
            _table = dynamodb.Table(table_name)
        return _table

    def flush_fn(observations: list[Observation]) -> None:
        flush_observations(observations, get_table(), ttl_days)

    return flush_fn


def fetch_service_data(table_name: str, service_name: str, region: str | None = None) -> list[dict]:
    """Query all caller/endpoint records for a given service."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    items: list[dict] = []
    kwargs: dict = {
        "KeyConditionExpression": Key("PK").eq(f"SERVICE#{service_name}")
    }

    while True:
        response = table.query(**kwargs)
        items.extend(response["Items"])
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items
