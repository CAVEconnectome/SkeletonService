from os import getenv
from messagingclient import MessagingClient
from .service import SkeletonService

def callback(payload):
    print("Skeleton Cache received message: ", payload)
    skeleton_params = {
        "datastack_name":      payload.attributes["skeleton_params_datastack_name"],
        "rid":                 int(payload.attributes["skeleton_params_rid"]),
        "materialize_version": int(payload.attributes["skeleton_params_materialize_version"]),
        "output_format":       payload.attributes["skeleton_params_output_format"],
        "sid":                 int(payload.attributes["skeleton_params_sid"]),
        "bucket":              payload.attributes["skeleton_params_bucket"],
        "root_resolution":     [int(v) for v in payload.attributes["skeleton_params_root_resolution"].split()],
        "collapse_soma":       False if payload.attributes["skeleton_params_collapse_soma"].lower() in ["false", "f", "0"] else True,
        "collapse_radius":     int(payload.attributes["skeleton_params_collapse_radius"]),
    }
    print("Skeleton Cache message parameters: ", skeleton_params)
    skel = SkeletonService.generate_skeleton(skeleton_params)

c = MessagingClient()
l2cache_update_queue = getenv("SKELETON_CACHE_RETRIEVE_QUEUE", "does-not-exist")
c.consume(l2cache_update_queue, callback)
