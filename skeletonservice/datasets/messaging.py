import os
import traceback as tb
import google.cloud.logging
import logging
from messagingclient import MessagingClientConsumer
from .service import SkeletonService

google.cloud.logging.Client().setup_logging()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('messagingclient')
logger.setLevel(logging.INFO)

env_verbose_level = int(os.environ.get('VERBOSE_LEVEL', "0"))

def callback(payload):
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
    except Exception as e:
        print("Skeleton Cache messaging message-processor suffered a failure that was not caught at lower granularity: ", repr(e))
        tb.print_exc()

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
