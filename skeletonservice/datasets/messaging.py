import os
import json
import traceback as tb
import logging
from timeit import default_timer
from messagingclient import MessagingClientConsumer
from messagingclient import RetryableError
from .service import SkeletonService, _PhaseTimer

# messagingclient logs one line per received message with the bare `logging` module, i.e. on the
# ROOT logger, not on a 'messagingclient' logger (see messagingclient/client.py, _consume_round_robin).
# Setting the named logger below therefore does nothing to it; the root level is what governs. At
# INFO that produced one "Received message ... b''." line per message from every worker. (The empty
# body is expected -- the producer puts everything in message attributes -- so it carries no
# information.) Default the root logger to WARNING and let LOG_LEVEL raise it for debugging.
logging.basicConfig(level=getattr(logging, os.environ.get('LOG_LEVEL', 'WARNING').upper(), logging.WARNING))

# Never let a logging handler failure surface as application noise or interrupt message processing.
logging.raiseExceptions = False

logger = logging.getLogger('messagingclient')
logger.setLevel(logging.INFO)

env_verbose_level = int(os.environ.get('VERBOSE_LEVEL', "0"))

# Mirror of service.log_phase_timings; see _PhaseTimer there.
log_phase_timings = os.environ.get('LOG_PHASE_TIMINGS', "false").lower() == "true"

# Statuses worth returning to the subscription rather than dropping the work. All are conditions
# that a later delivery can plausibly succeed at; anything else stays fatal.
_RETRYABLE_HTTP = {
    429: "Too Many Requests",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


try:
    from pcg_skel.chunk_tools import CompleteDataException
except ImportError:  # pcg_skel restructured; fall back to matching by name below.
    CompleteDataException = None


def _is_incomplete_l2_cache(exc):
    """True when the skeleton failed only because the l2 cache is not yet populated.

    pcg_skel raises CompleteDataException from dense_spatial_lookup when any level 2 id in the
    root lacks rep_coord_nm (require_complete=True). Requesting those ids is itself what makes
    pcgl2cache enqueue them for computation, so the work becomes possible a short time later --
    this is a wait, not a failure, and the message should come back rather than be dropped.

    Observed on minniev7: one missing id out of 13,065 fails the whole skeleton.
    """
    if CompleteDataException is not None and isinstance(exc, CompleteDataException):
        return True
    return type(exc).__name__ == "CompleteDataException"


def _retryable_status(exc):
    """Return the HTTP status this exception should be retried on, else None.

    The response status alone is not enough. The materialization service surfaces its rate
    limiter through a 500, so the exception observed in production reads:

        500 Server Error: 429 Too Many Requests: 800 per 1 minute for url: .../query

    i.e. the transport status is 500 while the actionable status is in the reason. Check the
    response first, then look for a standard reason phrase in the message.
    """
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in _RETRYABLE_HTTP:
        return status
    text = str(exc)
    for code, reason in _RETRYABLE_HTTP.items():
        if f"{code} {reason}" in text:
            return code
    return None

def callback(payload):
    # Wall time for the whole message, emitted on every path including failures. The service-level
    # PHASE_TIMINGS line only covers the preamble and generation; this bounds the rest (pull-to-ack
    # overhead, cache writes, serialization) so the two can be subtracted. Gated by LOG_PHASE_TIMINGS.
    message_start = default_timer()
    message_outcome = "ok"
    try:
        session_timestamp = payload.attributes["session_timestamp"]

        verbose_level = int(payload.attributes["verbose_level"])
        if env_verbose_level > verbose_level:
            verbose_level = env_verbose_level
        
        if verbose_level >= 1:
            s = ""
            for k in payload.attributes:
                s += f"\n |-- {k}: {payload.attributes[k]}"
            SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor received message: ", s, session_timestamp_=session_timestamp)
        
        subscription = "Unknown"
        try:
            subscription = payload.attributes.get("__subscription_name", "Unknown")
        except Exception as e:
            SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor error getting subscription from message: ", repr(e), session_timestamp_=session_timestamp)
            SkeletonService.print_with_session_timestamp(tb.format_exc(), session_timestamp_=session_timestamp)

        high_priority = None
        try:
            high_priority = payload.attributes["high_priority"]
        except Exception as e:
            SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor error getting priority from message: ", repr(e), session_timestamp_=session_timestamp)
            SkeletonService.print_with_session_timestamp(tb.format_exc(), session_timestamp_=session_timestamp)
        
        skeletoncache_dead_letter_queue = os.getenv("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", None)
        if verbose_level >= 1:
            SkeletonService.print_with_session_timestamp(f"Skeleton Cache message-processor subscription and high priority: {subscription}, {high_priority}", session_timestamp_=session_timestamp)
            SkeletonService.print_with_session_timestamp(f"Does the subscription ({subscription}) match the dead letter queue ({skeletoncache_dead_letter_queue})? {skeletoncache_dead_letter_queue in subscription}", session_timestamp_=session_timestamp)
        
        if skeletoncache_dead_letter_queue not in subscription:
            try:
                # NOTE: Forrest indicates I am shooting for something like the following once fully implemented.
                # SkelClassVsn = current_app.config['SKELETON_VERSION_ENGINES'][int(payload.attributes["skeleton_version"])]

                result = SkeletonService.get_skeleton_by_datastack_and_rid(
                    payload.attributes["skeleton_params_datastack_name"],
                    int(payload.attributes["skeleton_params_rid"]),
                    payload.attributes["skeleton_params_output_format"],
                    payload.attributes["skeleton_params_bucket"],
                    [int(v) for v in payload.attributes["skeleton_params_root_resolution"].split()],
                    False if payload.attributes["skeleton_params_collapse_soma"].lower() in ["false", "f", "0"] else True,
                    int(payload.attributes["skeleton_params_collapse_radius"]),
                    int(payload.attributes["skeleton_version"]),
                    False,  # via_requests
                    session_timestamp,
                    int(payload.attributes["verbose_level"]),
                )
                if verbose_level >= 1:
                    SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor returned from SkeletonService.get_skeleton_by_datastack_and_rid() with result: ", result, session_timestamp_=session_timestamp)
            except Exception as e:
                status = _retryable_status(e)
                if status is not None:
                    # Don't dump a traceback for these -- under a rate limit they are routine and
                    # the volume is exactly what made the real errors unreadable.
                    SkeletonService.print_with_session_timestamp(
                        f"Skeleton Cache message-processor got a retryable HTTP {status} from "
                        f"SkeletonService.get_skeleton_by_datastack_and_rid(); returning the message "
                        f"for redelivery.", session_timestamp_=session_timestamp)
                    raise RetryableError(f"HTTP {status}") from e
                if _is_incomplete_l2_cache(e):
                    # The read that just failed is also what queues the missing ids for
                    # computation, so redelivery is the retry. No traceback: this is an expected
                    # wait, and it was ~30% of messages while the trigger was broken.
                    SkeletonService.print_with_session_timestamp(
                        "Skeleton Cache message-processor hit an incomplete l2 cache for rid "
                        f"{payload.attributes['skeleton_params_rid']}; returning the message for "
                        "redelivery so pcgl2cache has time to compute the missing ids.",
                        session_timestamp_=session_timestamp)
                    raise RetryableError("incomplete l2 cache") from e
                SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor received error from SkeletonService.get_skeleton_by_datastack_and_rid(): ", repr(e), session_timestamp_=session_timestamp)
                SkeletonService.print_with_session_timestamp(tb.format_exc(), session_timestamp_=session_timestamp)
                raise e
        else:
            try:
                if verbose_level >= 1:
                    SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor received dead-letter message for datastack and rid: ",
                        payload.attributes["skeleton_params_datastack_name"], payload.attributes["skeleton_params_rid"], session_timestamp_=session_timestamp)
                    
                result = SkeletonService.add_rid_to_refusal_list(
                    payload.attributes["skeleton_params_bucket"],
                    payload.attributes["skeleton_params_datastack_name"],
                    int(payload.attributes["skeleton_params_rid"]),
                    int(payload.attributes["verbose_level"]),
                    # Carried on the message by retry_refusal_list. Absent for ordinary requests,
                    # so a first-time refusal records 0 and stays eligible for one retry.
                    retry_count=int(payload.attributes.get("refusal_retry_count", 0)),
                )
                if verbose_level >= 1:
                    SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor returned from SkeletonService.add_rid_to_refusal_list() with result: ", result, session_timestamp_=session_timestamp)
            except Exception as e:
                SkeletonService.print_with_session_timestamp("Skeleton Cache message-processor received error from SkeletonService.add_rid_to_refusal_list(): ", repr(e), session_timestamp_=session_timestamp)
                SkeletonService.print_with_session_timestamp(tb.format_exc(), session_timestamp_=session_timestamp)
                raise e
    except RetryableError:
        # Transient downstream failure (see _retryable_status). Let it reach the consumer, which
        # nacks the message so Pub/Sub redelivers it, and keeps this worker consuming. Without
        # this the catch-all below would swallow it, the message would be acked, and the work
        # would be lost -- which is what was happening to ~35% of messages under the
        # materialization rate limit.
        message_outcome = "retryable"
        raise
    except Exception as e:
        message_outcome = f"error:{type(e).__name__}"
        print("Skeleton Cache messaging message-processor suffered a failure that was not caught at lower granularity: ", repr(e))
        tb.print_exc()
    finally:
        # Only the early exits (refused / cache_hit / invalid) emitted their own PHASE_TIMINGS, so
        # generations -- the 150-800s messages we actually need to explain -- logged nothing. This
        # emits whatever the timer accumulated, on every path including failures.
        try:
            _PhaseTimer.emit_current(message_outcome)
        except Exception:
            pass  # instrumentation must never affect message handling
        if log_phase_timings:
            try:
                print("MESSAGE_TIMING " + json.dumps({
                    "outcome": message_outcome,
                    "total_s": round(default_timer() - message_start, 3),
                }), flush=True)
            except Exception:
                pass  # instrumentation must never affect message handling

# Which of the three subscriptions this process consumes: a comma separated selection of the keys
# low, high and dead. Mandatory -- there is no default, because silently falling back to all three
# would turn a chart that forgot to set it into a fleet quietly competing with the others.
#
# These are selector KEYS, not subscription names. The names come from the SKELETON_CACHE_*_
# RETRIEVE_QUEUE variables and are used verbatim (they are conventionally upper case, e.g.
# minniev7_SKELETON_CACHE_WORKER_LOW_PRIORITY); only the keys here are matched case-insensitively.
#
# One key per deployment gives a fleet per queue. That matters because the round-robin loop polls
# every configured subscription in turn and a blocking pull against an empty one costs the whole
# wait: measured on minniev7 2026-08-17, 50 workers sat at 23.5% occupancy with ~40s idle per
# message while thousands of messages waited. Separate fleets also scale on their own backlog, so
# the high-priority and dead-letter fleets can sit at zero while their queues are empty.
#
# All three queue-name variables must stay set whatever is consumed here: callback() distinguishes a
# dead-letter message from a skeleton request by matching the dead-letter queue name against the
# subscription the message arrived on.
CONSUME_QUEUES_ENV = "SKELETON_CACHE_CONSUME_QUEUES"

try:
    c = MessagingClientConsumer()
    skeletoncache_low_priority_queue = os.getenv("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", None)
    skeletoncache_high_priority_queue = os.getenv("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", None)
    skeletoncache_dead_letter_queue = os.getenv("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", None)
    if not skeletoncache_low_priority_queue or not skeletoncache_high_priority_queue or not skeletoncache_dead_letter_queue:
        raise ValueError(f"Skeleton Cache messaging client: one or more of the messaging queues are not set: LOW:{skeletoncache_low_priority_queue}, HIGH:{skeletoncache_high_priority_queue}, DEAD:{skeletoncache_dead_letter_queue}")

    available = {
        "low": skeletoncache_low_priority_queue,
        "high": skeletoncache_high_priority_queue,
        "dead": skeletoncache_dead_letter_queue,
    }
    raw = os.environ.get(CONSUME_QUEUES_ENV)
    if raw is None or not raw.strip():
        raise ValueError(
            f"{CONSUME_QUEUES_ENV} is required and must be a comma separated selection of "
            f"{sorted(available)} (it is not defaulted, so that a fleet cannot silently consume "
            f"queues it was not meant to)")
    keys = [k.strip().lower() for k in raw.split(",") if k.strip()]
    unknown = [k for k in keys if k not in available]
    if unknown:
        raise ValueError(
            f"{CONSUME_QUEUES_ENV}={raw!r} has unknown entries {unknown}; "
            f"valid keys are {sorted(available)}")
    if not keys:
        raise ValueError(f"{CONSUME_QUEUES_ENV}={raw!r} resolved to no queues")
    # dict.fromkeys: drop duplicates but keep the configured order, which is the round-robin order.
    keys = list(dict.fromkeys(keys))
    queues = [available[k] for k in keys]

    print(f"Skeleton Cache messaging client consuming {keys} -> {queues}")

    # consume_bounded rather than consume_multiple: the latter routes a single-queue list to
    # consume(), a streaming-pull path whose callback wrapper acks unconditionally and knows nothing
    # about RetryableError. A one-queue fleet would silently lose nack-based redelivery, the
    # 60s..600s backoff and the retryable/fatal distinction, waiting out the 600s ack deadline
    # instead. consume_bounded shares the round-robin engine and handles a list of length one, so
    # the semantics are identical however many queues are configured. Unbounded here (no
    # message_limit, no idle_timeout) matches the previous always-on behaviour.
    c.consume_bounded(queues, callback)
    print("Skeleton Cache messaging client registered callback successfully (barring any exceptions that are trapped inside MessagingClientConsumer).")
except Exception as e:
    print("Skeleton Cache messaging client failed to register callback: ", repr(e))
    tb.print_exc()
