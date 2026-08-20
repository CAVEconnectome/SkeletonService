"""Retryable failures must be returned to the subscription, not acked and dropped.

The consumer acks a message iff the callback returns without raising
(messagingclient.client._consume_round_robin). SkeletonService's callback had a catch-all that
swallowed every exception, so a failed message was acked and its work lost. Under the
materialization rate limit that was ~35% of all messages.

RetryableError now propagates out of the callback; the consumer nacks and keeps consuming.
"""

import os

import pytest
import requests

os.environ.setdefault("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", "low")
os.environ.setdefault("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", "high")
os.environ.setdefault("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", "dead")

from messagingclient import RetryableError  # noqa: E402
from skeletonservice.datasets import messaging  # noqa: E402


def _http_error(status, text):
    """An HTTPError shaped like the ones caveclient raises."""
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(text, response=response)


class TestRetryableClassification:
    def test_the_real_production_error_is_retryable(self):
        """Observed on minniev7: transport status 500, actionable status 429 in the text."""
        exc = _http_error(
            500,
            "500 Server Error: 429 Too Many Requests: 800 per 1 minute for url: "
            "https://minnie.microns-daf.com/materialize/api/v3/datastack/minnie65_phase3_v1/query",
        )

        assert messaging._retryable_status(exc) == 429

    @pytest.mark.parametrize(
        "status,reason",
        [(429, "Too Many Requests"), (502, "Bad Gateway"), (503, "Service Unavailable"), (504, "Gateway Timeout")],
    )
    def test_clean_statuses_are_retryable(self, status, reason):
        assert messaging._retryable_status(_http_error(status, f"{status} {reason}")) == status

    @pytest.mark.parametrize(
        "status,reason",
        [(429, "Too Many Requests"), (503, "Service Unavailable")],
    )
    def test_status_in_text_alone_is_enough(self, status, reason):
        """No response object at all -- some layers re-raise as a bare Exception."""
        assert messaging._retryable_status(Exception(f"500 Server Error: {status} {reason}: ...")) == status

    @pytest.mark.parametrize(
        "exc",
        [
            _http_error(500, "500 Internal Server Error"),
            _http_error(404, "404 Not Found"),
            _http_error(400, "400 Bad Request"),
            ValueError("something structural went wrong"),
            Exception("CompleteDataException"),
        ],
    )
    def test_genuine_failures_are_not_retryable(self, exc):
        """These must stay fatal; retrying them forever would hide real bugs."""
        assert messaging._retryable_status(exc) is None

    def test_a_root_id_containing_429_is_not_mistaken_for_a_rate_limit(self):
        assert messaging._retryable_status(Exception("failed for rid 864691135429000000")) is None


class TestCallbackPropagation:
    """The callback must re-raise RetryableError and swallow everything else."""

    class _Payload:
        def __init__(self):
            self.attributes = {
                "session_timestamp": "t",
                "verbose_level": "0",
                "__subscription_name": "projects/p/subscriptions/low",
                "high_priority": "false",
                "skeleton_params_datastack_name": "minnie65_public",
                "skeleton_params_rid": "864691135528193883",
                "skeleton_params_output_format": "none",
                "skeleton_params_bucket": "gs://bucket",
                "skeleton_params_root_resolution": "1 1 1",
                "skeleton_params_collapse_soma": "true",
                "skeleton_params_collapse_radius": "7500",
                "skeleton_version": "4",
            }

    def test_rate_limit_propagates_so_the_message_is_not_acked(self, monkeypatch):
        exc = _http_error(500, "500 Server Error: 429 Too Many Requests: 800 per 1 minute for url: x")
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: (_ for _ in ()).throw(exc)),
        )

        with pytest.raises(RetryableError):
            messaging.callback(self._Payload())

    def test_non_retryable_failure_is_still_swallowed(self, monkeypatch):
        """Unchanged behaviour: a genuine bug acks rather than hot-looping the subscription."""
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: (_ for _ in ()).throw(ValueError("structural"))),
        )

        messaging.callback(self._Payload())  # must not raise

    def test_success_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: None),
        )

        messaging.callback(self._Payload())

    def test_retryable_outcome_is_reported_in_message_timing(self, monkeypatch, capsys):
        monkeypatch.setattr(messaging, "log_phase_timings", True)
        exc = _http_error(429, "429 Too Many Requests")
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: (_ for _ in ()).throw(exc)),
        )

        with pytest.raises(RetryableError):
            messaging.callback(self._Payload())

        line = [l for l in capsys.readouterr().out.splitlines() if "MESSAGE_TIMING" in l]
        assert len(line) == 1, line
        import json
        assert json.loads(line[0].split("MESSAGE_TIMING ", 1)[1])["outcome"] == "retryable"


class TestIncompleteL2Cache:
    """A skeleton that fails only because the l2 cache is not populated must come back.

    pcg_skel raises CompleteDataException when any level 2 id in the root lacks rep_coord_nm.
    Requesting those ids is what makes pcgl2cache enqueue them, so the work becomes possible
    shortly afterwards -- dropping the message throws away a skeleton that would have succeeded
    on the next attempt. Measured on minniev7: 30% of generations, and one missing id out of
    13,065 is enough to fail the whole root.
    """

    def test_the_real_exception_is_recognised(self):
        from pcg_skel.chunk_tools import CompleteDataException

        exc = CompleteDataException("Some chunk indices are not yet computed")

        assert messaging._is_incomplete_l2_cache(exc) is True

    def test_recognised_by_name_when_the_import_is_unavailable(self, monkeypatch):
        """Guards the fallback: pcg_skel could restructure without us noticing."""
        monkeypatch.setattr(messaging, "CompleteDataException", None)

        class CompleteDataException(Exception):
            pass

        assert messaging._is_incomplete_l2_cache(CompleteDataException("x")) is True

    def test_a_message_that_merely_mentions_it_is_not_matched(self):
        """Discriminates on type, not text -- a rid or log line could contain the word."""
        assert messaging._is_incomplete_l2_cache(Exception("CompleteDataException")) is False
        assert messaging._is_incomplete_l2_cache(ValueError("structural")) is False

    def test_it_is_not_classified_as_an_http_retry(self):
        """Kept separate from _retryable_status; it is not an HTTP condition."""
        from pcg_skel.chunk_tools import CompleteDataException

        assert messaging._retryable_status(CompleteDataException("x")) is None

    def test_callback_returns_the_message_instead_of_dropping_it(self, monkeypatch):
        from pcg_skel.chunk_tools import CompleteDataException

        exc = CompleteDataException("Some chunk indices are not yet computed")
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: (_ for _ in ()).throw(exc)),
        )

        with pytest.raises(RetryableError):
            messaging.callback(TestCallbackPropagation._Payload())

    def test_outcome_is_reported_as_retryable(self, monkeypatch, capsys):
        """It must stop showing up as error:CompleteDataException in MESSAGE_TIMING."""
        from pcg_skel.chunk_tools import CompleteDataException

        monkeypatch.setattr(messaging, "log_phase_timings", True)
        monkeypatch.setattr(
            messaging.SkeletonService, "get_skeleton_by_datastack_and_rid",
            staticmethod(lambda *a, **k: (_ for _ in ()).throw(
                CompleteDataException("Some chunk indices are not yet computed"))),
        )

        with pytest.raises(RetryableError):
            messaging.callback(TestCallbackPropagation._Payload())

        import json
        line = [l for l in capsys.readouterr().out.splitlines() if "MESSAGE_TIMING" in l]
        assert len(line) == 1, line
        assert json.loads(line[0].split("MESSAGE_TIMING ", 1)[1])["outcome"] == "retryable"
