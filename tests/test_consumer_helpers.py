import pytest

import redis
from src.consumer import floor_bucket, ensure_group


def test_floor_bucket_5s():
    bucket_ms = 5000
    assert floor_bucket(0, bucket_ms) == 0
    assert floor_bucket(1, bucket_ms) == 0
    assert floor_bucket(4999, bucket_ms) == 0
    assert floor_bucket(5000, bucket_ms) == 5000
    assert floor_bucket(9999, bucket_ms) == 5000
    assert floor_bucket(12001, bucket_ms) == 10000


class FakeRedis:
    def __init__(self):
        self.created = []

    def xgroup_create(self, stream, group, id="0", mkstream=False):
        # record calls
        self.created.append((stream, group, id, mkstream))
        return "OK"


class FakeRedisBusy(FakeRedis):
    def xgroup_create(self, stream, group, id="0", mkstream=False):
        raise redis.ResponseError("BUSYGROUP Consumer Group name already exists")


def test_ensure_group_creates_if_missing():
    r = FakeRedis()
    ensure_group(r, "trades:btcusdt", "cg_analytics")
    assert r.created == [("trades:btcusdt", "cg_analytics", "0", True)]


def test_ensure_group_ignores_busygrouperror():
    r = FakeRedisBusy()
    # should not raise
    ensure_group(r, "trades:btcusdt", "cg_analytics")
