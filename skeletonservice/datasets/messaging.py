from os import getenv
import traceback as tb
from messagingclient import MessagingClient
from .service import SkeletonService

def callback(payload):
    verbose_level = int(payload.attributes["verbose_level"])
    if verbose_level >= 1:
        s = ""
        for k in payload.attributes:
            s += f"\n| {k}: {payload.attributes[k]}"
        print("Skeleton Cache message-processor received message: ", s)
    
    delivery_attempt = 0
    high_priority = True

    if verbose_level >= 1:
        try:
            print("Skeleton Cache message-processor retrieving message 'payload.delivery_attempt'...")
            print("Skeleton Cache message-processor message attempt 1: ", type(payload.delivery_attempt), payload.delivery_attempt)
            delivery_attempt = payload.delivery_attempt
        except Exception as e:
            print("Skeleton Cache message-processor message attempt 1 error: ", repr(e))
            print(tb.format_exc())
    
    if verbose_level >= 1:
        try:
            print("Skeleton Cache message-processor retrieving message 'payload.attributes.googleclient_deliveryattempt'...")
            print("Skeleton Cache message-processor message attempt 2: ", type(payload.attributes.googleclient_deliveryattempt), payload.attributes.googleclient_deliveryattempt)
        except Exception as e:
            print("Skeleton Cache message-processor message attempt 2 error: ", repr(e))
            print(tb.format_exc())
    
    if verbose_level >= 1:
        try:
            print("Skeleton Cache message-processor retrieving priority...")
            print("Skeleton Cache message-processor message high priority: ", payload.attributes["high_priority"])
        except Exception as e:
            print("Skeleton Cache message-processor message high priority error: ", repr(e))
            print(tb.format_exc())
    
    if verbose_level >= 1:
        print("Skeleton Cache message-processor message delivery attempt and high priority: ", delivery_attempt, high_priority)
    
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

c = MessagingClient()
l2cache_low_priority_update_queue = getenv("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
l2cache_high_priority_update_queue = getenv("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
l2cache_low_dead_update_queue = getenv("SKELETON_CACHE_DEAD_LETTER_RETRIEVE_QUEUE", "does-not-exist")
c.consume_multiple([l2cache_low_priority_update_queue,
                    l2cache_high_priority_update_queue,
                    l2cache_low_dead_update_queue,
                    l2cache_low_dead_update_queue],
                    callback)
