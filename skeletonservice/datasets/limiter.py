from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import g
import os
import json
from skeletonservice.datasets.service import NEUROGLANCER_SKELETON_VERSION, SkeletonService

def limit_by_category(category):
    limit = get_rate_limit_from_config(category)
    if limit is not None:
        return limiter.limit(limit, key_func=lambda: g.auth_user["id"])
    return lambda x: x

def limit_by_skeleton_exists(request):
    """
    Apply rate limiting based on the category and check if the skeleton exists for the root ID in the request.
    """
    try:
        root_id = request.args.get("rid")
        if not root_id:
            raise ValueError("Root ID is missing from the request.")
        datastack_name = request.args.get("datastack_name")
        if not datastack_name:
            raise ValueError("Datastack name is missing from the request.")
        skvn = request.args.get("skvn")
        if not skvn:
            # raise ValueError("Skeleton version is missing from the request.")
            print("Limiter.limit_by_skeleton_exists(): Skeleton version is missing from the request. Using default version.")
            skvn = NEUROGLANCER_SKELETON_VERSION
    except RuntimeError as e:
        print(f"Limiter.limit_by_skeleton_exists(): Error parsing request arguments: {e}")
        return lambda x: x
    
    print(f"Limiter.limit_by_skeleton_exists(): successfully parsed request arguments: root id: {root_id}, datastack: {datastack_name}, skeleton_version: {skvn}")

    # Check if the skeleton exists
    bucket = os.environ.get("SKELETON_BUCKET", "default_bucket")
    sk_exists = SkeletonService.skeletons_exist(bucket, datastack_name, int(skvn), int(root_id))

    # Retrieve the rate limit for the category
    limit = get_rate_limit_from_config("sk_exists" if sk_exists else "sk_not_exists")
    print(f"Limiter.limit_by_skeleton_exists(): root id: {root_id}, datastack: {datastack_name}, skeleton_version: {skvn}, bucket: {bucket}, skeleton exists: {sk_exists}, limit: {limit}")
    if limit is not None:
        return limiter.limit(limit, key_func=lambda: g.auth_user["id"])
    return lambda x: x

def get_rate_limit_from_config(category=None):
    if category:
        categories_str = os.environ.get("LIMITER_CATEGORIES", "{}")
        print(f"Limiter.get_rate_limit_from_config(): categories_str: {categories_str}")
        if not categories_str:
            # None, "", {} : The environment variable was probably populated with an empty string during deployment
            # such that it isn't literally "None", but JSON won't read an empty string, so it's just as bad as None.
            return None
        
        categories_dict = json.loads(categories_str)
        print(f"Limiter.get_rate_limit_from_config(): categories_dict: {categories_dict}")
        if not categories_dict:
            # None, "", {} : The environment variable was probably populated with an empty string during deployment
            # such that it isn't literally "None", but JSON won't read an empty string, so it's just as bad as None.
            return None
        if category not in categories_dict:
            return None  # Default rate limit if not found
        
        return categories_dict[category]
    else:
        return None

limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
