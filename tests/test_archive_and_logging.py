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
import json
import logging
from unittest import mock

import pytest


@pytest.fixture
def service(monkeypatch):
    """Import service.py fresh so module-level env reads are re-evaluated.

    importlib.reload() re-executes the module into its existing namespace, replacing the
    SkeletonService class object. test_skeletonservice.py did `from ... import SkeletonService`
    at collection time and still holds the original, so after a reload the class it patches is
    no longer the one the service code calls -- which sent seven of its tests to real GCS.

    The original class object stays alive (its subclasses reference it), so restoring the
    snapshotted namespace afterwards puts it back and keeps the reload inside this test.
    """
    import skeletonservice.datasets.service as svc

    original = svc.__dict__.copy()

    def _load(**env):
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        return importlib.reload(svc)

    yield _load

    svc.__dict__.clear()
    svc.__dict__.update(original)


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
    """The worker must not install a Cloud Logging handler.

    GKE already ships container stdout to Cloud Logging, so a handler only duplicated the
    shipping -- and setup_logging() was called from four modules, attaching several handlers to
    the ROOT logger. Every emit then failed with a 403 because the worker credentials lack
    logging.logEntries.create.
    """

    MODULES = [
        "skeletonservice",
        "skeletonservice.datasets.service",
        "skeletonservice.datasets.api",
        "skeletonservice.datasets.messaging",
    ]
    SOURCES = [
        "skeletonservice/__init__.py",
        "skeletonservice/datasets/service.py",
        "skeletonservice/datasets/api.py",
        "skeletonservice/datasets/messaging.py",
    ]

    @pytest.mark.parametrize("source", SOURCES)
    def test_source_has_no_setup_logging_call(self, source):
        from pathlib import Path

        text = Path(source).read_text()
        assert "google.cloud.logging.Client()" not in text
        assert "setup_logging()" not in text

    @pytest.mark.parametrize("module", MODULES)
    def test_imports_without_google_cloud_logging_installed(self, module):
        """The strongest form of the assertion: the package must not need the dependency."""
        importlib.import_module(module)  # must not raise

    def test_root_logger_has_no_cloud_logging_handler(self):
        for module in self.MODULES:
            importlib.import_module(module)
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


class TestPhaseTimerCoversGeneration:
    """The generation path must emit, since it is the only expensive one.

    Live evidence (minniev7, 2026-08-17): 449 messages in 30 min, 145 of them real generations at
    p50 145s / max 795s -- and every PHASE_TIMINGS line was outcome=cache_hit, because only the
    early exits called emit(). The slow path, the one worth measuring, logged nothing.
    """

    def test_marks_reach_the_timer_without_being_passed_it(self, service):
        """Nested code (pcg_skel helpers) marks via the thread-local rather than a new argument."""
        svc = service(LOG_PHASE_TIMINGS="true")

        t = svc._PhaseTimer(1)
        svc._PhaseTimer.mark_current("gen_pcg_meshwork")

        assert "gen_pcg_meshwork" in t._phases

    def test_emit_current_emits_when_nothing_else_did(self, service, capsys):
        svc = service(LOG_PHASE_TIMINGS="true")

        svc._PhaseTimer(864691135528193883)
        svc._PhaseTimer.emit_current("generated")

        out = [l for l in capsys.readouterr().out.splitlines() if "PHASE_TIMINGS" in l]
        assert len(out) == 1, out
        assert json.loads(out[0].split("PHASE_TIMINGS ", 1)[1])["outcome"] == "generated"

    def test_emit_current_does_not_double_emit(self, service, capsys):
        """Early exits already emit; the finally-block fallback must not add a second line."""
        svc = service(LOG_PHASE_TIMINGS="true")

        t = svc._PhaseTimer(1)
        t.emit("cache_hit")
        svc._PhaseTimer.emit_current("ok")

        lines = [l for l in capsys.readouterr().out.splitlines() if "PHASE_TIMINGS" in l]
        assert len(lines) == 1, lines
        assert json.loads(lines[0].split("PHASE_TIMINGS ", 1)[1])["outcome"] == "cache_hit"

    def test_emit_current_is_safe_with_no_timer(self, service):
        svc = service(LOG_PHASE_TIMINGS="true")
        svc._PhaseTimer._current.timer = None

        svc._PhaseTimer.emit_current("ok")  # must not raise

    def test_repeated_phase_accumulates(self, service):
        """A retried call should add to its phase, not silently overwrite the earlier time."""
        svc = service(LOG_PHASE_TIMINGS="true")

        t = svc._PhaseTimer(1)
        t.mark("gen_pcg_meshwork")
        first = t._phases["gen_pcg_meshwork"]
        t.mark("gen_pcg_meshwork")

        assert t._phases["gen_pcg_meshwork"] >= first

    def test_v4_generation_is_instrumented(self):
        """Guard the marks themselves: the black box must stay broken open."""
        from pathlib import Path
        import re

        src = Path("skeletonservice/datasets/service.py").read_text()
        start = src.index("def _generate_v4_skeleton")
        body = src[start:start + 12000]
        for phase in ("gen_root_soma", "gen_pcg_meshwork", "gen_volumetric_props",
                      "gen_segment_props"):
            assert f'mark_current("{phase}")' in body, phase
