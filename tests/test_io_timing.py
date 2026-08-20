"""Tests for per-dependency I/O timing in _PhaseTimer.

Why this exists: on minniev7 2026-08-20 a median skeleton took 3.67s while the worker used only
~0.15 core-seconds of CPU, so ~96% of it was spent waiting -- but the phase timers could not say on
what. Attributing it by multiplying fleet-wide ingress percentiles by calls-per-skeleton did not
close: 4.02 materialize calls at the fleet p50 of 0.509s already exceeds the 1.34s gen_root_soma
phase that contains them. That inference is also what led to blaming pcg-read for gen_pcg_meshwork
when pcg-read turned out to be ~1% of the budget. This measures each wait in-process.
"""

import json
import time
from unittest.mock import patch

import pytest
import requests

from skeletonservice.datasets import service as svc
from skeletonservice.datasets.service import _PhaseTimer


@pytest.fixture(autouse=True)
def timings_on(monkeypatch):
    monkeypatch.setattr(svc, "log_phase_timings", True)
    yield
    _PhaseTimer._current.timer = None


def emitted_payload(capsys):
    """The JSON from the single PHASE_TIMINGS line."""
    out = capsys.readouterr().out
    line = [l for l in out.splitlines() if "PHASE_TIMINGS" in l]
    assert line, f"no PHASE_TIMINGS emitted; got: {out[:400]}"
    return json.loads(line[-1].split("PHASE_TIMINGS ", 1)[1])


class TestBucketClassification:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://minnie.microns-daf.com/materialize/api/v3/x", "materialize"),
            ("https://minnie.microns-daf.com/segmentation/api/v1/y", "pcg"),
            ("https://minnie.microns-daf.com/l2cache/api/v1/z", "l2cache"),
            ("https://global.daf-apis.com/info/api/v2/q", "info"),
            ("https://global.daf-apis.com/auth/api/v1/user", "auth"),
            ("https://global.daf-apis.com/schema/type", "schema"),
            ("https://storage.googleapis.com/bucket/obj", "gcs"),
            ("https://oauth2.googleapis.com/token", "google"),
            ("https://example.org/whatever", "other"),
        ],
    )
    def test_classifies_by_path_not_only_host(self, url, expected):
        """materialize, pcg and l2cache share a hostname; only the path separates them."""
        assert svc._io_bucket(url) == expected

    def test_never_raises_on_junk(self):
        for bad in ["", "not a url", None, 12345]:
            assert svc._io_bucket(bad) == "other"


class TestAccumulation:
    def test_seconds_and_counts_accumulate_per_dependency(self, capsys):
        t = _PhaseTimer(864691135406097394)
        t.add_io("materialize", 0.5)
        t.add_io("materialize", 0.25)
        t.add_io("gcs", 0.125)
        t.emit("ok")
        p = emitted_payload(capsys)
        assert p["io_materialize_s"] == 0.75 and p["io_materialize_n"] == 2
        assert p["io_gcs_s"] == 0.125 and p["io_gcs_n"] == 1

    def test_unaccounted_is_reported_explicitly(self, capsys):
        """The remainder must be named, not left for the reader to subtract."""
        t = _PhaseTimer(1)
        t.add_io("materialize", 0.01)
        t.emit("ok")
        p = emitted_payload(capsys)
        assert "io_total_s" in p and "io_unaccounted_s" in p
        # total_s >= io_total_s, and the three are self-consistent
        assert abs(p["io_total_s"] + p["io_unaccounted_s"] - p["total_s"]) < 0.01

    def test_no_io_keys_when_nothing_was_called(self, capsys):
        _PhaseTimer(1).emit("ok")
        p = emitted_payload(capsys)
        assert not [k for k in p if k.startswith("io_")]

    def test_disabled_when_phase_timings_off(self, monkeypatch, capsys):
        monkeypatch.setattr(svc, "log_phase_timings", False)
        t = _PhaseTimer(1)
        t.add_io("materialize", 1.0)
        t.emit("ok")
        assert "PHASE_TIMINGS" not in capsys.readouterr().out


class TestHttpShim:
    def test_shim_is_installed_once_and_idempotent(self):
        from requests.adapters import HTTPAdapter

        assert getattr(HTTPAdapter, "_skeletonservice_io_timed", False)
        before = HTTPAdapter.send
        svc._install_io_timing()
        assert HTTPAdapter.send is before, "re-installing must not double-wrap"

    def test_a_real_request_is_attributed_to_its_dependency(self, capsys):
        """Drive the actual requests stack; only the network call is stubbed."""
        t = _PhaseTimer(1)
        url = "https://minnie.microns-daf.com/materialize/api/v3/datastack/x/query"
        delay = 0.05

        def slow(*a, **kw):
            time.sleep(delay)
            raise requests.exceptions.ConnectionError("stubbed")

        with patch("urllib3.connectionpool.HTTPConnectionPool.urlopen", side_effect=slow):
            with pytest.raises(Exception):
                requests.Session().get(url, timeout=1)
        t.emit("ok")
        p = emitted_payload(capsys)
        assert p.get("io_materialize_n") == 1, p
        # Must record the real wait, not just that a call happened.
        assert p["io_materialize_s"] >= delay, p
        assert p["io_unaccounted_s"] < p["total_s"], p

    def test_failed_calls_are_still_counted(self, capsys):
        """A timeout is the case most worth attributing, so it must not be dropped."""
        t = _PhaseTimer(1)
        with patch("urllib3.connectionpool.HTTPConnectionPool.urlopen") as uo:
            uo.side_effect = requests.exceptions.ConnectTimeout("stubbed")
            with pytest.raises(Exception):
                requests.Session().get("https://storage.googleapis.com/b/o", timeout=1)
        t.emit("ok")
        p = emitted_payload(capsys)
        assert p.get("io_gcs_n") == 1, p

    def test_requests_outside_a_message_are_ignored(self, capsys):
        """Flask API paths and background threads have no timer; they must not crash or leak."""
        _PhaseTimer._current.timer = None
        with patch("urllib3.connectionpool.HTTPConnectionPool.urlopen") as uo:
            uo.side_effect = requests.exceptions.ConnectionError("stubbed")
            with pytest.raises(Exception):
                requests.Session().get("https://example.org/x", timeout=1)
        # nothing emitted, nothing raised
        assert "PHASE_TIMINGS" not in capsys.readouterr().out
