from os import getenv
from messagingclient import MessagingClient
import skeletonservice as sksv

def callback(payload):
    print("Skeleton Cache received message: ", payload)
    skeleton_params_strs = payload.attributes['skeleton_params']
    skeleton_params = {
        "datastack_name":      skeleton_params_strs["datastack_name"],
        "rid":                 int(skeleton_params_strs["rid"]),
        "materialize_version": int(skeleton_params_strs["materialize_version"]),
        "output_format":       skeleton_params_strs["output_format"],
        "sid":                 int(skeleton_params_strs["sid"]),
        "bucket":              skeleton_params_strs["bucket"],
        "root_resolution":     [int(v) for v in skeleton_params_strs["root_resolution"].split()],
        "collapse_soma":       False if skeleton_params_strs["collapse_soma"].lower() in ["false", "f", "0"] else True,
        "collapse_radius":     int(skeleton_params_strs["collapse_radius"]),
    }
    skel = sksv.generate_skeleton(skeleton_params)

c = MessagingClient()
l2cache_update_queue = getenv("SKELETON_CACHE_UPDATE_QUEUE", "does-not-exist")
c.consume(l2cache_update_queue, callback)
