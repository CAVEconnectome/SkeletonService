"""Guards for two throughput regressions found on minniev7 (2026-08-16).

1. _archive_skeletonization_time() rewrote a single bucket-root CSV in full on every generated
   skeleton. The object had reached 85,309 rows / 8.10 MB uncompressed and all 200 workers
   contended for it, leaving them at 1-6 millicores with ~9,000 messages backlogged. It is now
   opt-in via ARCHIVE_SKELETONIZATION_TIMES.

2. google.cloud.logging setup_logging() was called at import scope in four modules, attaching
   several handlers to the root logger; each emit then failed with a 403 because the worker
   credentials lack logging.logEntries.create. GKE already ships stdout, so no handler is needed.
"""

import importlib
import logging
from unittest import mock

import pytest


@pytest.fixture
def service(monkeypatch):
    """Import service.py fresh so module-level env reads are re-evaluated."""

    def _load(**env):
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        import skeletonservice.datasets.service as svc

        return importlib.reload(svc)

    return _load


class TestArchiveKillSwitch:
    def test_disabled_by_default_touches_no_storage(self, service, monkeypatch):
        monkeypatch.delenv("ARCHIVE_SKELETONIZATION_TIMES", raising=False)
        svc = service()

        assert svc.archive_skeletonization_times is False
        with mock.patch.object(svc, "CloudFiles") as cf:
            svc.SkeletonService._archive_skeletonization_time(
                "gs://bucket", "minnie65_public", 864691135528193883, 4, 100, 5, 3, 12.5
            )
        cf.assert_not_called()

    @pytest.mark.parametrize("value", ["false", "False", "0", "no", ""])
    def test_other_values_stay_disabled(self, service, value):
        assert service(ARCHIVE_SKELETONIZATION_TIMES=value).archive_skeletonization_times is False

    @pytest.mark.parametrize("value", ["true", "True", "TRUE"])
    def test_opt_in_reenables_the_old_path(self, service, value):
        svc = service(ARCHIVE_SKELETONIZATION_TIMES=value)

        assert svc.archive_skeletonization_times is True
        with mock.patch.object(svc, "CloudFiles") as cf:
            cf.return_value.exists.return_value = False
            svc.SkeletonService._archive_skeletonization_time(
                "gs://bucket", "minnie65_public", 864691135528193883, 4, 100, 5, 3, 12.5
            )
        cf.assert_called_once()
        cf.return_value.put.assert_called_once()

    def test_header_columns_match_the_appended_row(self, service):
        """A from-scratch file previously got a header whose column order did not match the rows."""
        svc = service(ARCHIVE_SKELETONIZATION_TIMES="true")

        with mock.patch.object(svc, "CloudFiles") as cf:
            cf.return_value.exists.return_value = False  # force the header branch
            svc.SkeletonService._archive_skeletonization_time(
                "gs://bucket", "minnie65_public", 864691135528193883, 4, 100, 5, 3, 12.5
            )

        written = cf.return_value.put.call_args[0][1].decode("utf-8")
        header, row = written.splitlines()[0].split(","), written.splitlines()[1].split(",")
        assert len(header) == len(row), f"{len(header)} header cols vs {len(row)} row cols"
        assert header.index("Datastack_Name") < header.index("Skeleton_Version")
        assert row[header.index("Datastack_Name")] == "minnie65_public"
        assert row[header.index("Skeleton_Version")] == "4"
        assert row[header.index("Root_ID")] == "864691135528193883"

    def test_failures_still_do_not_propagate(self, service):
        """Archiving is best-effort; a storage error must not fail the skeleton."""
        svc = service(ARCHIVE_SKELETONIZATION_TIMES="true")

        with mock.patch.object(svc, "CloudFiles", side_effect=RuntimeError("boom")):
            svc.SkeletonService._archive_skeletonization_time(
                "gs://bucket", "minnie65_public", 1, 4, 1, 1, 1, 1.0
            )  # must not raise


class TestNoCloudLoggingHandler:
    MODULES = [
        "skeletonservice",
        "skeletonservice.datasets.service",
        "skeletonservice.datasets.api",
        "skeletonservice.datasets.messaging",
    ]

    @pytest.mark.parametrize("module", MODULES)
    def test_module_does_not_install_a_cloud_logging_handler(self, module):
        """setup_logging() attaches to the ROOT logger, so importing must not add one."""
        with mock.patch("google.cloud.logging.Client") as client:
            try:
                importlib.reload(importlib.import_module(module))
            except Exception as exc:  # import may fail for unrelated env reasons; the assert below still holds
                pytest.skip(f"{module} not importable in this environment: {exc!r}")
        client.assert_not_called()

    def test_root_logger_has_no_cloud_logging_handler(self):
        names = [type(h).__name__ for h in logging.getLogger().handlers]
        assert "CloudLoggingHandler" not in names, names
        assert "StructuredLogHandler" not in names, names


class TestPhaseTimer:
    """_PhaseTimer instruments the worker preamble, which runs on every message."""

    def test_disabled_by_default_emits_nothing(self, service, monkeypatch, capsys):
        monkeypatch.delenv("LOG_PHASE_TIMINGS", raising=False)
        svc = service()

        assert svc.log_phase_timings is False
        t = svc._PhaseTimer(864691135528193883)
        t.mark("refusal_list")
        t.emit("cache_hit")
        assert "PHASE_TIMINGS" not in capsys.readouterr().out

    def test_enabled_emits_one_parseable_line(self, service, capsys):
        svc = service(LOG_PHASE_TIMINGS="true")

        t = svc._PhaseTimer(864691135528193883)
        t.mark("refusal_list")
        t.mark("caveclient_init")
        t.emit("cache_hit")

        out = [l for l in capsys.readouterr().out.splitlines() if "PHASE_TIMINGS" in l]
        assert len(out) == 1, out
        import json as _json
        payload = _json.loads(out[0].split("PHASE_TIMINGS ", 1)[1])
        assert payload["rid"] == "864691135528193883"
        assert payload["outcome"] == "cache_hit"
        assert {"refusal_list", "caveclient_init", "total_s"} <= set(payload)
        assert all(isinstance(payload[k], (int, float)) for k in ("refusal_list", "total_s"))

    def test_emits_at_most_once_per_message(self, service, capsys):
        svc = service(LOG_PHASE_TIMINGS="true")

        t = svc._PhaseTimer(1)
        t.emit("cache_hit")
        t.emit("generated")

        assert len([l for l in capsys.readouterr().out.splitlines() if "PHASE_TIMINGS" in l]) == 1

    def test_never_raises(self, service):
        """Instrumentation must not be able to fail a message."""
        svc = service(LOG_PHASE_TIMINGS="true")

        class Unserializable:
            def __str__(self):
                raise ValueError("nope")

        t = svc._PhaseTimer(Unserializable())
        t.mark("x")
        t.emit("ok")  # must not raise
