"""Refused roots get a bounded second chance instead of being blacklisted forever.

The list was append-only with no removal path anywhere in the codebase, so anything that
dead-lettered was refused permanently. An audit of the live list found 8 of 17 entries would
succeed today -- they had been refused for an incomplete l2 cache, which is now fixed and
retried. Two of those had 21 and 26 level-2 ids, so they were never "too big to skeletonize".

The retry is deliberately built on the existing dead-letter path: retry_refusal_list removes the
row and re-queues it, and if it fails again add_rid_to_refusal_list puts it back. The bound comes
from RETRY_COUNT travelling on the message, so the count is not reset when it is re-added.
"""

from io import BytesIO
from unittest import mock

import pandas as pd
import pytest

from skeletonservice.datasets.service import (
    REFUSAL_RETRY_COUNT_COLUMN,
    SKELETONIZATION_REFUSAL_LIST_FILENAME,
    SkeletonService,
)

BUCKET = "gs://test_bucket"
DS = "minnie65_phase3_v1"


def _csv(rows, with_count=True):
    cols = ["TIMESTAMP", "DATASTACK_NAME", "ROOT_ID"] + ([REFUSAL_RETRY_COUNT_COLUMN] if with_count else [])
    return pd.DataFrame(rows, columns=cols).to_csv(index=False)


@pytest.fixture
def cf():
    """Patch CloudFiles, serving a refusal list and capturing writes."""
    with mock.patch("skeletonservice.datasets.service.CloudFiles") as CF:
        state = {"csv": _csv([["20260101_000000", DS, 111, 0]])}

        def get(name):
            return state["csv"].encode("utf-8")

        def put(name, data, **kw):
            state["csv"] = data.decode("utf-8") if isinstance(data, bytes) else data
            return "ok"

        CF.return_value.exists.side_effect = lambda name: True
        CF.return_value.get.side_effect = get
        CF.return_value.put.side_effect = put
        CF.state = state
        yield CF


class TestBackwardCompatibility:
    def test_a_file_without_the_column_reads_as_never_retried(self, cf):
        cf.state["csv"] = _csv([["20260101_000000", DS, 111]], with_count=False)

        df = SkeletonService._read_refusal_list(BUCKET)

        assert REFUSAL_RETRY_COUNT_COLUMN in df.columns
        assert df[REFUSAL_RETRY_COUNT_COLUMN].tolist() == [0]

    def test_the_refusal_check_still_works_with_the_extra_column(self, cf):
        """The check compares positionally against [datastack, rid]; an extra column broke it."""
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is True
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 999)) is False

    def test_identity_frame_has_exactly_the_two_identity_columns(self, cf):
        df = SkeletonService._read_refusal_list_without_timestamps(BUCKET)

        assert list(df.columns) == ["DATASTACK_NAME", "ROOT_ID"]


class TestRemoval:
    def test_removing_an_entry_takes_it_off_the_list(self, cf):
        removed = SkeletonService.remove_rids_from_refusal_list(BUCKET, [(DS, 111)])

        assert len(removed) == 1
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is False

    def test_removing_something_absent_writes_nothing(self, cf):
        before = cf.state["csv"]

        removed = SkeletonService.remove_rids_from_refusal_list(BUCKET, [(DS, 999)])

        assert removed.empty
        assert cf.state["csv"] == before, "must not rewrite the object for a no-op"

    def test_only_the_named_entry_is_removed(self, cf):
        cf.state["csv"] = _csv([["t", DS, 111, 0], ["t", DS, 222, 0], ["t", "other", 111, 0]])

        SkeletonService.remove_rids_from_refusal_list(BUCKET, [(DS, 111)])

        df = SkeletonService._read_refusal_list(BUCKET)
        assert sorted(zip(df["DATASTACK_NAME"], df["ROOT_ID"])) == [(DS, 222), ("other", 111)]


class TestRetry:
    @pytest.fixture(autouse=True)
    def _publisher(self):
        with mock.patch("skeletonservice.datasets.service.MessagingClientPublisher") as P:
            self.published = []
            P.return_value.publish.side_effect = lambda ex, payload, attrs: self.published.append(attrs)
            yield P

    def test_an_eligible_root_is_removed_and_requeued(self, cf):
        requeued = SkeletonService.retry_refusal_list(BUCKET)

        assert len(requeued) == 1
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is False, \
            "must come off the list, or the retry is refused on arrival"
        assert len(self.published) == 1
        assert self.published[0]["skeleton_params_rid"] == "111"

    def test_the_requeued_message_carries_an_incremented_count(self, cf):
        SkeletonService.retry_refusal_list(BUCKET)

        assert self.published[0]["refusal_retry_count"] == "1"

    def test_an_already_retried_root_is_not_retried_again(self, cf):
        cf.state["csv"] = _csv([["t", DS, 111, 1]])

        requeued = SkeletonService.retry_refusal_list(BUCKET, max_retry_count=1)

        assert requeued.empty
        assert self.published == []
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is True, \
            "an exhausted root must stay refused"

    def test_max_retry_count_can_allow_more_attempts(self, cf):
        cf.state["csv"] = _csv([["t", DS, 111, 1]])

        assert len(SkeletonService.retry_refusal_list(BUCKET, max_retry_count=2)) == 1
        assert self.published[0]["refusal_retry_count"] == "2"

    def test_dry_run_changes_nothing(self, cf):
        before = cf.state["csv"]

        eligible = SkeletonService.retry_refusal_list(BUCKET, dry_run=True)

        assert len(eligible) == 1
        assert cf.state["csv"] == before
        assert self.published == []

    def test_datastack_filter_limits_the_scope(self, cf):
        cf.state["csv"] = _csv([["t", DS, 111, 0], ["t", "other_stack", 222, 0]])

        SkeletonService.retry_refusal_list(BUCKET, datastack_name=DS)

        assert [a["skeleton_params_rid"] for a in self.published] == ["111"]
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, "other_stack", 222)) is True

    def test_an_empty_list_is_a_no_op(self, cf):
        cf.state["csv"] = _csv([])

        assert SkeletonService.retry_refusal_list(BUCKET).empty
        assert self.published == []


class TestPutBackOnRepeatedFailure:
    """The dead-letter path is the 'put it back' half; it must preserve the count."""

    def test_refusing_again_records_the_count_from_the_message(self, cf):
        cf.state["csv"] = _csv([])

        SkeletonService.add_rid_to_refusal_list(BUCKET, DS, 111, retry_count=1)

        df = SkeletonService._read_refusal_list(BUCKET)
        assert df[REFUSAL_RETRY_COUNT_COLUMN].tolist() == [1]

    def test_a_first_time_refusal_records_zero_and_stays_eligible(self, cf):
        cf.state["csv"] = _csv([])

        SkeletonService.add_rid_to_refusal_list(BUCKET, DS, 111)

        df = SkeletonService._read_refusal_list(BUCKET)
        assert df[REFUSAL_RETRY_COUNT_COLUMN].tolist() == [0]

    def test_full_cycle_retry_then_fail_leaves_it_permanently_refused(self, cf):
        """retry -> removed -> fails -> re-added with count 1 -> no longer eligible."""
        with mock.patch("skeletonservice.datasets.service.MessagingClientPublisher") as P:
            sent = []
            P.return_value.publish.side_effect = lambda ex, p, a: sent.append(a)
            SkeletonService.retry_refusal_list(BUCKET)

        # the retry failed and dead-lettered, carrying the count back
        SkeletonService.add_rid_to_refusal_list(
            BUCKET, DS, 111, retry_count=int(sent[0]["refusal_retry_count"]))

        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is True
        with mock.patch("skeletonservice.datasets.service.MessagingClientPublisher"):
            assert SkeletonService.retry_refusal_list(BUCKET).empty, "must not retry a second time"


class TestSweepBounds:
    """A scheduled sweep must be bounded in both dimensions.

    Pub/Sub redelivery covers minutes to hours. This sweep covers days, for roots refused because a
    long outage exhausted those attempts. But a root can fail by *hanging* -- a materialize query
    that OOMs and never answers -- and each of its delivery attempts then occupies a worker for the
    full 600s ack deadline. So limit caps roots per sweep, and max_retry_count caps sweeps per root.
    """

    @pytest.fixture(autouse=True)
    def _publisher(self):
        with mock.patch("skeletonservice.datasets.service.MessagingClientPublisher") as P:
            self.published = []
            P.return_value.publish.side_effect = lambda ex, payload, attrs: self.published.append(attrs)
            yield P

    def test_limit_caps_how_many_are_requeued(self, cf):
        cf.state["csv"] = _csv([[f"2026010{i}_000000", DS, 100 + i, 0] for i in range(1, 8)])

        requeued = SkeletonService.retry_refusal_list(BUCKET, limit=3)

        assert len(requeued) == 3
        assert len(self.published) == 3

    def test_limit_takes_the_oldest_first(self, cf):
        cf.state["csv"] = _csv([
            ["20260301_000000", DS, 303, 0],
            ["20260101_000000", DS, 101, 0],
            ["20260201_000000", DS, 202, 0],
        ])

        SkeletonService.retry_refusal_list(BUCKET, limit=2)

        assert [a["skeleton_params_rid"] for a in self.published] == ["101", "202"]

    def test_the_remainder_stays_on_the_list_for_the_next_sweep(self, cf):
        cf.state["csv"] = _csv([["20260101_000000", DS, 101, 0], ["20260201_000000", DS, 202, 0]])

        SkeletonService.retry_refusal_list(BUCKET, limit=1)

        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 202)) is True
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 101)) is False

    def test_no_limit_requeues_everything_eligible(self, cf):
        cf.state["csv"] = _csv([["t", DS, 100 + i, 0] for i in range(5)])

        assert len(SkeletonService.retry_refusal_list(BUCKET, limit=None)) == 5

    def test_a_weeks_worth_of_sweeps_then_left_alone(self, cf):
        """max_retry_count=7 => seven daily chances, then the root stops being retried."""
        cf.state["csv"] = _csv([["t", DS, 111, 7]])

        assert SkeletonService.retry_refusal_list(BUCKET, max_retry_count=7).empty
        assert self.published == []
        assert bool(SkeletonService._check_root_id_against_refusal_list(BUCKET, DS, 111)) is True

    def test_sweeps_default_to_low_priority(self, cf):
        """A sweep of old failures must not compete with interactive requests."""
        SkeletonService.retry_refusal_list(BUCKET)

        assert self.published[0]["high_priority"] == "False"


class TestSweepCli:
    def test_the_cli_refuses_to_guess_the_bucket(self, monkeypatch, capsys):
        """Sweeping the wrong bucket would re-queue nothing and look like success."""
        monkeypatch.delenv("SKELETON_CACHE_BUCKET", raising=False)
        from skeletonservice.scripts import retry_refusal_list as cli

        rc = cli.main([])

        assert rc == 2
        assert "--bucket" in capsys.readouterr().err

    def test_the_cli_passes_its_arguments_through(self, cf):
        from skeletonservice.scripts import retry_refusal_list as cli

        with mock.patch.object(SkeletonService, "retry_refusal_list",
                               return_value=SkeletonService._read_refusal_list(BUCKET).head(0)) as m:
            cli.main(["--bucket", BUCKET, "--max-retry-count", "3", "--limit", "5", "--dry-run"])

        kwargs = m.call_args.kwargs
        assert kwargs["max_retry_count"] == 3
        assert kwargs["limit"] == 5
        assert kwargs["dry_run"] is True
