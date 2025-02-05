from os import getenv
import traceback as tb
from messagingclient import MessagingClient
from .service import SkeletonService

def callback(payload):
    print("Skeleton Cache message-processer received message: ", payload)
    try:
        # NOTE: Forrest indicates I am shooting for something like the following once fully implemented.
        # SkelClassVsn = current_app.config['SKELETON_VERSION_ENGINES'][int(payload.attributes["skeleton_version"])]

        result = SkeletonService.get_skeleton_by_datastack_and_rid(
            payload.attributes["skeleton_params_datastack_name"],
            int(payload.attributes["skeleton_params_rid"]),
            None,  # output_format
            payload.attributes["skeleton_params_bucket"],
            [int(v) for v in payload.attributes["skeleton_params_root_resolution"].split()],
            False if payload.attributes["skeleton_params_collapse_soma"].lower() in ["false", "f", "0"] else True,
            int(payload.attributes["skeleton_params_collapse_radius"]),
            int(payload.attributes["skeleton_version"]),
            False,  # via_requests
            int(payload.attributes["verbose_level"]),
        )
        print("Skeleton Cache message-processer returned from SkeletonService.get_skeleton_by_datastack_and_rid() with result: ", result)
    except Exception as e:
        print("Error generating skeleton: ", repr(e))
        print(tb.format_exc())
        raise e

c = MessagingClient()
l2cache_low_priority_update_queue = getenv("SKELETON_CACHE_LOW_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
l2cache_high_priority_update_queue = getenv("SKELETON_CACHE_HIGH_PRIORITY_RETRIEVE_QUEUE", "does-not-exist")
c.consume_multiple([l2cache_low_priority_update_queue, l2cache_high_priority_update_queue], callback)
