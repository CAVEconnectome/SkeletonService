from os import getenv
from messagingclient import MessagingClient
from .service import SkeletonService

def callback(payload):
    print("Skeleton Cache received message: ", payload)
    skeleton_params = [
        int(payload.attributes["skeleton_params_rid"]),
        payload.attributes["skeleton_params_bucket"],
        payload.attributes["skeleton_params_datastack_name"],
        int(payload.attributes["skeleton_params_materialize_version"]),
        [int(v) for v in payload.attributes["skeleton_params_root_resolution"].split()],
        False if payload.attributes["skeleton_params_collapse_soma"].lower() in ["false", "f", "0"] else True,
        int(payload.attributes["skeleton_params_collapse_radius"]),
        
    ]
    print("Skeleton Cache message parameters: ", skeleton_params)

    try:
        result = SkeletonService.get_skeleton_by_datastack_and_rid(
            payload.attributes["skeleton_params_datastack_name"],
            int(payload.attributes["skeleton_params_rid"]),
            None,  # output_format
            payload.attributes["skeleton_params_bucket"],
            [int(v) for v in payload.attributes["skeleton_params_root_resolution"].split()],
            False if payload.attributes["skeleton_params_collapse_soma"].lower() in ["false", "f", "0"] else True,
            int(payload.attributes["skeleton_params_collapse_radius"]),
        )
        print("Skeleton Cache generated skeleton with ", skeleton.n_vertices, " vertices")
    except Exception as e:
        print("Error generating skeleton: ", e)

c = MessagingClient()
l2cache_update_queue = getenv("SKELETON_CACHE_RETRIEVE_QUEUE", "does-not-exist")
c.consume(l2cache_update_queue, callback)
