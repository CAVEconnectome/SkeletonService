"""Tests for the cached CAVEclient factory.

Why this exists: caveclient memoises `.materialize` and `.materialize.tables` per INSTANCE, so
constructing a client per message re-bootstraps the TableManager every time -- version discovery,
table metadata for each soma table, schema population. Measured on minniev7 2026-08-19 that was
five sequential HTTP round trips per skeleton, ~55% of materialize's entire request volume, all
inside the gen_root_soma phase.
"""

import threading
import time
from unittest.mock import patch

import pytest

from skeletonservice.datasets import service as svc
from messagingclient import RetryableError

from skeletonservice.datasets.service import SkeletonService


@pytest.fixture(autouse=True)
def clear_cache():
    svc._cave_client_cache.clear()
    yield
    svc._cave_client_cache.clear()


class FakeMaterialize:
    def __init__(self, api_version):
        self.api_version = api_version


class FakeClient:
    """Stands in for CAVEclient; counts how many were built.

    `api_version` mirrors what caveclient resolves at construction. It defaults to 3 because a
    client that resolved 2 is rejected by the guard in _build_checked_cave_client -- caveclient
    falls back to 2 when version discovery fails, and such a client raises
    ServerIncompatibilityError on every `.tables` access.
    """

    instances = 0
    api_version = 3

    def __init__(self, datastack_name, server_address=None):
        FakeClient.instances += 1
        self.datastack_name = datastack_name
        self.server_address = server_address
        self.id = FakeClient.instances
        self.materialize = FakeMaterialize(type(self).api_version)


@pytest.fixture
def fake_caveclient(monkeypatch):
    FakeClient.instances = 0
    FakeClient.api_version = 3
    monkeypatch.setattr(svc.caveclient, "CAVEclient", FakeClient)
    return FakeClient


class TestReuse:
    def test_second_call_reuses_the_same_instance(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        a = SkeletonService._get_cave_client("minnie65_phase3_v1")
        b = SkeletonService._get_cave_client("minnie65_phase3_v1")
        assert a is b, "a per-message client rebuilds TableManager and costs 5 round trips"
        assert fake_caveclient.instances == 1

    def test_many_calls_build_exactly_one(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        for _ in range(50):
            SkeletonService._get_cave_client("minnie65_phase3_v1")
        assert fake_caveclient.instances == 1

    def test_distinct_datastacks_are_not_shared(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        a = SkeletonService._get_cave_client("minnie65_phase3_v1")
        b = SkeletonService._get_cave_client("minnie65_public")
        assert a is not b
        assert fake_caveclient.instances == 2

    def test_distinct_servers_are_not_shared(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        a = SkeletonService._get_cave_client("ds", server_address="https://a")
        b = SkeletonService._get_cave_client("ds", server_address="https://b")
        assert a is not b and fake_caveclient.instances == 2

    def test_default_server_address_is_the_module_constant(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        c = SkeletonService._get_cave_client("ds")
        assert c.server_address == svc.CAVE_CLIENT_SERVER


class TestStaleness:
    def test_client_is_rebuilt_after_the_ttl(self, fake_caveclient, monkeypatch):
        """TableManager caches table metadata and the version list, so reuse must be bounded."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        clock = [1000.0]
        monkeypatch.setattr(svc.time, "monotonic", lambda: clock[0])
        first = SkeletonService._get_cave_client("ds")
        clock[0] += 299
        assert SkeletonService._get_cave_client("ds") is first
        clock[0] += 2  # now past the TTL
        second = SkeletonService._get_cave_client("ds")
        assert second is not first
        assert fake_caveclient.instances == 2

    def test_ttl_zero_disables_caching(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 0)
        a = SkeletonService._get_cave_client("ds")
        b = SkeletonService._get_cave_client("ds")
        assert a is not b
        assert fake_caveclient.instances == 2
        assert svc._cave_client_cache == {}, "disabled caching must not populate the cache"


class TestConcurrency:
    def test_threads_converge_on_one_client(self, fake_caveclient, monkeypatch):
        """Racing threads may build more than one, but all callers must end up sharing one.

        Convergence is what matters: two callers holding different instances would each carry
        their own TableManager and pay the bootstrap separately.
        """
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        got = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            got.append(SkeletonService._get_cave_client("ds"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(got) == 8
        cached = svc._cave_client_cache[("ds", svc.CAVE_CLIENT_SERVER)][0]
        # Every subsequent caller gets the cached instance.
        assert SkeletonService._get_cave_client("ds") is cached

    def test_construction_does_not_hold_the_lock(self, fake_caveclient, monkeypatch):
        """Construction does network I/O; holding the lock across it would serialise workers."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        held_during_build = []

        class SlowClient(FakeClient):
            def __init__(self, *a, **kw):
                held_during_build.append(svc._cave_client_cache_lock.locked())
                super().__init__(*a, **kw)

        monkeypatch.setattr(svc.caveclient, "CAVEclient", SlowClient)
        SkeletonService._get_cave_client("ds")
        assert held_during_build == [False]


class TestApiVersionGuard:
    """A client whose version discovery fell back must never be cached.

    caveclient's _api_endpoints(..., fallback_version=2, ...) silently degrades to api_version 2
    when it cannot reach the version endpoint; MaterializationClient.tables then raises
    ServerIncompatibilityError on every access. Caching such a client turned one transient blip
    into 17,570 acked-and-dropped messages on minniev7 2026-08-20.
    """

    def test_v2_client_raises_and_is_not_cached(self, fake_caveclient, monkeypatch):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        fake_caveclient.api_version = 2
        with pytest.raises(RetryableError):
            SkeletonService._get_cave_client("ds")
        assert svc._cave_client_cache == {}, (
            "a rejected client must leave no cache entry, or it poisons the whole TTL"
        )

    def test_raises_retryable_so_the_message_is_redelivered_not_dropped(
        self, fake_caveclient, monkeypatch
    ):
        """RetryableError is what messaging.callback nacks on; anything else is acked and lost."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        fake_caveclient.api_version = 2
        with pytest.raises(RetryableError):
            SkeletonService._get_cave_client("ds")

    @pytest.mark.parametrize("version", [3, "3", "3.0.0", 4])
    def test_usable_versions_are_accepted_and_cached(self, fake_caveclient, monkeypatch, version):
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        fake_caveclient.api_version = version
        c = SkeletonService._get_cave_client("ds")
        assert SkeletonService._get_cave_client("ds") is c
        assert fake_caveclient.instances == 1

    def test_unreadable_materialize_is_rejected(self, monkeypatch):
        """If the materialize sub-client cannot even be built, that is not usable either."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)

        class Exploding:
            def __init__(self, datastack_name, server_address=None):
                pass

            @property
            def materialize(self):
                raise RuntimeError("datastack info unreachable")

        monkeypatch.setattr(svc.caveclient, "CAVEclient", Exploding)
        with pytest.raises(RetryableError):
            SkeletonService._get_cave_client("ds")
        assert svc._cave_client_cache == {}

    def test_ttl_zero_path_also_validates(self, fake_caveclient, monkeypatch):
        """Caching disabled must not mean validation disabled."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 0)
        fake_caveclient.api_version = 2
        with pytest.raises(RetryableError):
            SkeletonService._get_cave_client("ds")

    def test_a_rejection_does_not_poison_later_attempts(self, fake_caveclient, monkeypatch):
        """The whole point: recovery must be immediate once discovery works again."""
        monkeypatch.setattr(svc, "CAVE_CLIENT_CACHE_TTL_S", 300)
        fake_caveclient.api_version = 2
        with pytest.raises(RetryableError):
            SkeletonService._get_cave_client("ds")
        fake_caveclient.api_version = 3
        good = SkeletonService._get_cave_client("ds")
        assert SkeletonService._get_cave_client("ds") is good


class TestCallSites:
    def test_no_direct_construction_remains(self):
        """Every generation path must go through the factory, or it silently pays the cost."""
        import inspect
        import re

        src = inspect.getsource(svc)
        body = src.split("def _get_cave_client", 1)[1]
        # skip the factory itself, then check the rest of the module
        rest = body.split("\n    @staticmethod", 1)[1] if "\n    @staticmethod" in body else ""
        assert "caveclient.CAVEclient(" not in rest, (
            "a direct caveclient.CAVEclient(...) outside the factory rebuilds TableManager"
        )
        assert len(re.findall(r"SkeletonService\._get_cave_client\(", src)) >= 6
