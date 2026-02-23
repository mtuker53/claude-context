from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from claude_context.capture.observation import AggregatedObservation
from claude_context.storage.dynamo import fetch_service_data, flush_observations, write_observation

TABLE_NAME = "claude-context-test"
REGION = "us-east-1"


@pytest.fixture
def dynamo_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


def _agg(**kwargs) -> AggregatedObservation:
    now = datetime.now(timezone.utc)
    defaults = dict(
        service_name="my-api",
        caller="checkout",
        method="POST",
        path_template="/api/orders",
        request_fields={"user_id", "cart_id"},
        request_headers={"x-correlation-id"},
        query_params=set(),
        response_codes={"200"},
        call_count=5,
        first_seen=now,
        last_seen=now,
    )
    defaults.update(kwargs)
    return AggregatedObservation(**defaults)


class TestWriteObservation:
    def test_writes_record(self, dynamo_table):
        write_observation(dynamo_table, _agg())
        response = dynamo_table.get_item(
            Key={"PK": "SERVICE#my-api", "SK": "CALLER#checkout#POST#/api/orders"}
        )
        item = response["Item"]
        assert item["call_count"] == Decimal(5)
        assert "user_id" in item["request_fields"]
        assert "200" in item["response_codes"]

    def test_accumulates_on_repeated_writes(self, dynamo_table):
        write_observation(dynamo_table, _agg(call_count=3))
        write_observation(dynamo_table, _agg(call_count=2))
        item = dynamo_table.get_item(
            Key={"PK": "SERVICE#my-api", "SK": "CALLER#checkout#POST#/api/orders"}
        )["Item"]
        assert item["call_count"] == Decimal(5)

    def test_accumulates_new_fields(self, dynamo_table):
        write_observation(dynamo_table, _agg(request_fields={"user_id"}))
        write_observation(dynamo_table, _agg(request_fields={"cart_id"}))
        item = dynamo_table.get_item(
            Key={"PK": "SERVICE#my-api", "SK": "CALLER#checkout#POST#/api/orders"}
        )["Item"]
        assert "user_id" in item["request_fields"]
        assert "cart_id" in item["request_fields"]

    def test_skips_empty_sets(self, dynamo_table):
        # Should not raise even when sets are empty
        agg = _agg(request_fields=set(), request_headers=set(), query_params=set())
        write_observation(dynamo_table, agg)
        item = dynamo_table.get_item(
            Key={"PK": "SERVICE#my-api", "SK": "CALLER#checkout#POST#/api/orders"}
        )["Item"]
        assert "request_fields" not in item


class TestFetchServiceData:
    def test_returns_all_records_for_service(self, dynamo_table):
        write_observation(dynamo_table, _agg(caller="checkout"))
        write_observation(dynamo_table, _agg(caller="mobile-bff"))

        with mock_aws():
            # Re-use the existing mock context by querying directly
            items = dynamo_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key("PK").eq("SERVICE#my-api")
            )["Items"]

        assert len(items) == 2

    def test_returns_empty_for_unknown_service(self, dynamo_table):
        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name=REGION)
            items = fetch_service_data(
                table_name=TABLE_NAME,
                service_name="nonexistent",
                region=REGION,
            )
        assert items == []
