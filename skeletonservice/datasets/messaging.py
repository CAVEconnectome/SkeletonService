import os
import json
import traceback as tb
import logging
from timeit import default_timer
from messagingclient import MessagingClientConsumer
from messagingclient import RetryableError
from .service import SkeletonService

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
        if log_phase_timings:
            try:
                print("MESSAGE_TIMING " + json.dumps({
                    "outcome": message_outcome,
                    "total_s": round(default_timer() - message_start, 3),
                }), flush=True)
            except Exception:
                pass  # instrumentation must never affect message handling

try:
    c = MessagingClientConsumer()
    skeletoncache_low_priority_queue = os.getenv("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", None)
    skeletoncache_high_priority_queue = os.getenv("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", None)
    skeletoncache_dead_letter_queue = os.getenv("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", None)
    if not skeletoncache_low_priority_queue or not skeletoncache_high_priority_queue or not skeletoncache_dead_letter_queue:
        raise ValueError(f"Skeleton Cache messaging client: one or more of the messaging queues are not set: LOW:{skeletoncache_low_priority_queue}, HIGH:{skeletoncache_high_priority_queue}, DEAD:{skeletoncache_dead_letter_queue}")
    c.consume_multiple([skeletoncache_low_priority_queue,
                        skeletoncache_high_priority_queue,
                        skeletoncache_dead_letter_queue],
                        callback)
    print("Skeleton Cache messaging client registered callback successfully (barring any exceptions that are trapped inside MessagingClientConsumer).")
except Exception as e:
    print("Skeleton Cache messaging client failed to register callback: ", repr(e))
    tb.print_exc()
