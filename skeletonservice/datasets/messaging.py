from os import getenv
from messagingclient import MessagingClient
import skeletonservice as sksv

def callback(payload):
    print("Skeleton Cache received message: ", payload)
    skel = sksv.generate_skeleton(payload['skeleton_params'])

c = MessagingClient()
l2cache_update_queue = getenv("SKELETON_CACHE_UPDATE_QUEUE", "does-not-exist")
c.consume(l2cache_update_queue, callback)
