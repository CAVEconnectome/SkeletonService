"""Re-attempt refused root ids on a slow schedule.

Two timescales, deliberately:

* Minutes to hours -- Pub/Sub redelivery. A nacked message is retried up to
  skeletoncache_max_delivery_attempts times with exponential backoff (~5 hours at 50 attempts).
  This absorbs transient downstream failures: a rate limit, an l2 cache that has not computed its
  level 2 ids yet, or a materialize outage. When it runs out the message dead-letters and the root
  is added to the refusal list.

* Days -- this script. Dead-lettering means "retries exhausted", not "this root cannot be
  skeletonized", and the refusal list is consulted on every request, so a root refused during a
  long outage would never be attempted again. Each sweep gives refused roots one more chance and
  increments their RETRY_COUNT; once that reaches --max-retry-count the root is left alone, so a
  genuinely broken root (an uncomputable statistic, a query that OOMs the database) stops costing
  anything after a bounded number of days rather than being retried forever.

A root that succeeds is simply gone from the list. A root that fails again dead-letters and is
re-added with its count preserved, which is what bounds the whole thing.

    python -m skeletonservice.scripts.retry_refusal_list --bucket gs://minnie65_skeletons --dry-run
"""

import argparse
import os
import sys


def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=os.environ.get("SKELETON_CACHE_BUCKET"),
                   help="skeleton cache bucket, e.g. gs://minnie65_skeletons "
                        "(default: $SKELETON_CACHE_BUCKET)")
    p.add_argument("--datastack", default=None, help="only retry this datastack")
    p.add_argument("--max-retry-count", type=int,
                   default=int(os.environ.get("REFUSAL_RETRY_MAX_COUNT", 7)),
                   help="give each root at most this many sweeps before leaving it refused "
                        "(default: 7, i.e. a week of daily sweeps)")
    p.add_argument("--limit", type=int,
                   default=int(os.environ.get("REFUSAL_RETRY_LIMIT", 25)),
                   help="most roots to re-queue in one sweep, oldest first (default: 25)")
    p.add_argument("--high-priority", action="store_true",
                   help="publish to the high priority exchange (default: low, so a sweep cannot "
                        "starve interactive requests)")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would be re-queued and change nothing")
    p.add_argument("--verbose", type=int, default=1)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not args.bucket:
        print("error: --bucket or $SKELETON_CACHE_BUCKET is required", file=sys.stderr)
        return 2

    from skeletonservice.datasets.service import (
        REFUSAL_RETRY_COUNT_COLUMN,
        SkeletonService,
    )

    before = SkeletonService._read_refusal_list(args.bucket)
    print(f"refusal list: {len(before)} entries in {args.bucket}")
    if len(before):
        counts = before[REFUSAL_RETRY_COUNT_COLUMN].value_counts().sort_index().to_dict()
        print(f"  by retry count: {counts}")

    requeued = SkeletonService.retry_refusal_list(
        args.bucket,
        datastack_name=args.datastack,
        max_retry_count=args.max_retry_count,
        limit=args.limit,
        high_priority=args.high_priority,
        dry_run=args.dry_run,
        verbose_level_=args.verbose,
    )

    verb = "would re-queue" if args.dry_run else "re-queued"
    print(f"{verb} {len(requeued)} root id(s) (max_retry_count={args.max_retry_count}, "
          f"limit={args.limit})")
    for _, row in requeued.iterrows():
        print(f"  {row['ROOT_ID']}  {row['DATASTACK_NAME']}  "
              f"refused={row['TIMESTAMP']}  attempt={int(row[REFUSAL_RETRY_COUNT_COLUMN]) + 1}")
    if not len(requeued):
        print("  nothing eligible; every refused root has used its sweeps or the list is empty")
    return 0


if __name__ == "__main__":
    sys.exit(main())
