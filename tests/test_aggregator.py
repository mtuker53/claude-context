from datetime import datetime, timezone

from claude_context.capture.aggregator import aggregate
from claude_context.capture.observation import Observation


def _obs(**kwargs) -> Observation:
    defaults = dict(
        service_name="my-api",
        caller="checkout",
        method="POST",
        path_template="/api/orders",
        request_fields=frozenset({"user_id"}),
        request_headers=frozenset(),
        query_params=frozenset(),
        status_code=200,
        timestamp=datetime.now(timezone.utc),
    )
    defaults.update(kwargs)
    return Observation(**defaults)


class TestAggregate:
    def test_single_observation(self):
        result = aggregate([_obs()])
        assert len(result) == 1
        assert result[0].call_count == 1

    def test_merges_same_key(self):
        obs1 = _obs(request_fields=frozenset({"user_id"}), status_code=200)
        obs2 = _obs(request_fields=frozenset({"cart_id"}), status_code=422)
        result = aggregate([obs1, obs2])
        assert len(result) == 1
        agg = result[0]
        assert agg.call_count == 2
        assert "user_id" in agg.request_fields
        assert "cart_id" in agg.request_fields
        assert "200" in agg.response_codes
        assert "422" in agg.response_codes

    def test_separates_different_callers(self):
        obs1 = _obs(caller="checkout")
        obs2 = _obs(caller="mobile-bff")
        result = aggregate([obs1, obs2])
        assert len(result) == 2

    def test_separates_different_endpoints(self):
        obs1 = _obs(method="POST", path_template="/api/orders")
        obs2 = _obs(method="GET", path_template="/api/orders/{id}")
        result = aggregate([obs1, obs2])
        assert len(result) == 2

    def test_tracks_first_and_last_seen(self):
        t1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        obs1 = _obs(timestamp=t2)
        obs2 = _obs(timestamp=t1)
        result = aggregate([obs1, obs2])
        agg = result[0]
        assert agg.first_seen == t1
        assert agg.last_seen == t2

    def test_empty_input(self):
        assert aggregate([]) == []
