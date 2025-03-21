from os import getenv
import traceback as tb
import logging
from messagingclient import MessagingClient
from .service import SkeletonService

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger('messagingclient')
logger.setLevel(logging.INFO)

def callback(payload):
    verbose_level = int(payload.attributes["verbose_level"])
    if verbose_level >= 1:
        s = ""
        for k in payload.attributes:
            s += f"\n| {k}: {payload.attributes[k]}"
        print("Skeleton Cache message-processor received message: ", s)
    
    subscription = "Unknown"
    try:
        subscription = payload.attributes.get("__subscription_name", "Unknown")
    except Exception as e:
        print("Skeleton Cache message-processor error getting subscription from message: ", repr(e))
        print(tb.format_exc())

    high_priority = None
    try:
        high_priority = payload.attributes["high_priority"]
    except Exception as e:
        print("Skeleton Cache message-processor error getting priority from message: ", repr(e))
        print(tb.format_exc())
    
    if verbose_level >= 1:
        print(f"Skeleton Cache message-processor subscription and high priority: {subscription}, {high_priority}")
        print(f"Does the subscription match the dead letter queue ({l2cache_dead_update_queue})? {subscription} == {l2cache_dead_update_queue}")
    
    if subscription != l2cache_dead_update_queue:
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
                int(payload.attributes["verbose_level"]),
            )
            if verbose_level >= 1:
                print("Skeleton Cache message-processor returned from SkeletonService.get_skeleton_by_datastack_and_rid() with result: ", result)
        except Exception as e:
            print("Skeleton Cache message-processor received error from SkeletonService.get_skeleton_by_datastack_and_rid(): ", repr(e))
            print(tb.format_exc())
            raise e
    else:
        try:
            if verbose_level >= 1:
                print("Skeleton Cache message-processor received dead-letter message for datastack and rid: ",
                    payload.attributes["skeleton_params_datastack_name"], payload.attributes["skeleton_params_rid"])
                
            result = SkeletonService.add_rid_to_refusal_list(
                payload.attributes["skeleton_params_bucket"],
                payload.attributes["skeleton_params_datastack_name"],
                int(payload.attributes["skeleton_params_rid"]),
                int(payload.attributes["verbose_level"]),
            )
            if verbose_level >= 1:
                print("Skeleton Cache message-processor returned from SkeletonService.add_rid_to_refusal_list() with result: ", result)
        except Exception as e:
            print("Skeleton Cache message-processor received error from SkeletonService.add_rid_to_refusal_list(): ", repr(e))
            print(tb.format_exc())
            raise e

c = MessagingClient()
l2cache_low_priority_update_queue = getenv("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
l2cache_high_priority_update_queue = getenv("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
l2cache_dead_update_queue = getenv("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", "does-not-exist")
c.consume_multiple([l2cache_low_priority_update_queue,
                    l2cache_high_priority_update_queue,
                    l2cache_dead_update_queue],
                    callback)
