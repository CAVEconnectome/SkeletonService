from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import current_app, g
import os
import json

from skeletonservice.datasets.service import NEUROGLANCER_SKELETON_VERSION, SkeletonService

# An empty context manager to use when the limiter is not configured,
# such as when running tests or in a local development environment.
class EmptyContextManager:
    """A context manager that does nothing."""
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass
    
    def __call__(self, func, skvn=None):
        print(f"EmptyContextManager called with arg: {func}")
        return lambda datastack_name: True

mock_limiter = EmptyContextManager()
limit_uri = os.environ.get("LIMITER_URI", "LIMITER_URI not set")
DEBUG = (limit_uri == "LIMITER_URI not set" or limit_uri == "None")

def get_rate_limit_from_config(category=None):
    if not category:
        return None
    
    categories_str = os.environ.get("LIMITER_CATEGORIES", "{}")
    # print(f"Limiter.get_rate_limit_from_config(): categories_str 1: {categories_str}")
    categories_str = categories_str.replace("'", '"')
    # print(f"Limiter.get_rate_limit_from_config(): categories_str 2: {categories_str}")

    if not categories_str:  # Catch any "False" equivalent: None, "", {}, [], etc.
        # The environment variable was probably populated with an empty string during deployment
        # such that it isn't literally "None", but the json library won't read an empty string, so it's just as bad as None.
        return None
    
    categories_dict = json.loads(categories_str)
    print(f"Limiter.get_rate_limit_from_config(): categories_dict: {categories_dict}")
    if not categories_dict or category not in categories_dict:
        return None  # Default rate limit if not found
    
    print(f"Limiter.get_rate_limit_from_config(): limit for category {category}: {categories_dict.get(category)}")
    return categories_dict.get(category)

def apply_rate_limit(limit):
    if limit is not None:
        return limiter.limit(limit, key_func=lambda: g.auth_user["id"])
    if DEBUG:
        return mock_limiter
    # I don't believe this lambda approach is compatible. The mock_limiter should work better, but this was the original approach taken from another repo.
    return lambda x: x

def limit_by_category(category):
    return apply_rate_limit(get_rate_limit_from_config(category))



# Placeholder: the endpoint in api.py that would use this limiter simply replies on limit_by_category() instead.
# def limit_query_cache(request):
#     return apply_rate_limit(get_rate_limit_from_config("query_cache"))

# Placeholder: the endpoint in api.py that would use this limiter simply replies on limit_by_category() instead.
# def limit_skeleton_exists(request):
#     return apply_rate_limit(get_rate_limit_from_config("skeleton_exists"))

def limit_get_skeleton(request, via_msg=False):
    try:
        root_id = request.args.get("rid")  # POST args
        if not root_id:
            root_id = request.view_args.get("rid")  # GET args
            if not root_id:
                raise ValueError("Root ID is missing from the request.")
        datastack_name = request.args.get("datastack_name")
        if not datastack_name:
            datastack_name = request.view_args.get("datastack_name")
            if not datastack_name:
                raise ValueError("Datastack name is missing from the request.")
        skvn = request.args.get("skvn")
        if not skvn:
            skvn = request.view_args.get("skvn")
            if not skvn:
                print("Limiter.limit_get_skeleton(): Skeleton version is missing from the request. Using default version.")
                skvn = NEUROGLANCER_SKELETON_VERSION
    except RuntimeError as e:
        print(f"Limiter.limit_get_skeleton(): Error parsing request arguments: {e}")
        if DEBUG:
            return mock_limiter
        # I don't believe this lambda approach is compatible. The mock_limiter should work better, but this was the original approach taken from another repo.
        return lambda x: x
    
    print(f"Limiter.limit_get_skeleton(): successfully parsed request arguments: root id: {root_id}, datastack: {datastack_name}, skeleton_version: {skvn}")

    # Check if the skeleton exists
    bucket = current_app.config["SKELETON_CACHE_BUCKET"]
    # bucket = os.environ.get("SKELETON_CACHE_BUCKET", "default_bucket")
    sk_exists = SkeletonService.skeletons_exist(bucket, datastack_name, int(skvn), int(root_id))

    # Retrieve the rate limit for the category
    if not via_msg:
        category = "get_skeleton_that_exists" if sk_exists else "get_skeleton_that_doesnt_exist"
    else:
        category = "get_skeleton_via_msg_that_exists" if sk_exists else "get_skeleton_via_msg_that_doesnt_exist"
    limit = get_rate_limit_from_config(category)
    print(f"Limiter.limit_get_skeleton(): root id: {root_id}, datastack: {datastack_name}, skeleton_version: {skvn}, bucket: {bucket}, skeleton exists: {sk_exists}, limit: {limit}")
    return apply_rate_limit(limit)

def limit_get_skeletons_via_msg(request):
    # We can reuse the basic skeleton limit for this endpoint
    return limit_get_skeleton(request, via_msg=True)

# Placeholder: the endpoint in api.py that would use this limiter simply replies on limit_by_category() instead.
# def limit_get_skeletons_bulk(request):
#     return apply_rate_limit(get_rate_limit_from_config("get_skeletons_bulk"))

def limit_get_skeleton_async(request):
    # We can reuse the basic skeleton limit for this endpoint
    if DEBUG:
        return mock_limiter
    return limit_get_skeleton(request)

# On localhost:5000, the following output is printed:
    # Initializing Limiter with LIMITER_URI env. var.: 'None_C'
    # Initializing Limiter with LIMITER_CATEGORIES env. var.: 'LIMITER_CATEGORIES not set'
    # Initializing Limiter with AUTH_URI env. var.: 'global.daf-apis.com/auth'
    # Initializing Limiter with SKELETON_CACHE_LOW_PRIORITY_EXCHANGE env. var.: 'ltv5_SKELETON_CACHE_LOW_PRIORITY'
# On the ltv server, the following output is printed:
    # Initializing Limiter with LIMITER_URI env. var.: 'LIMITER_URI not set'
    # Initializing Limiter with LIMITER_CATEGORIES env. var.: 'LIMITER_CATEGORIES not set'
    # Initializing Limiter with AUTH_URI env. var.: 'global.daf-apis.com/auth'
    # Initializing Limiter with SKELETON_CACHE_LOW_PRIORITY_EXCHANGE env. var.: 'ltv5_SKELETON_CACHE_LOW_PRIORITY'

# Placeholder: the endpoint in api.py that would use this limiter simply replies on limit_by_category() instead.
# def limit_gen_skeletons_bulk_async(request):
#     return apply_rate_limit(get_rate_limit_from_config("get_skeletons_bulk_async"))
print(f'Initializing Limiter with LIMITER_URI env. var.: \'{os.environ.get("LIMITER_URI", "LIMITER_URI not set")}\'')

print(f'Initializing Limiter with LIMITER_CATEGORIES env. var.: \'{os.environ.get("LIMITER_CATEGORIES", "LIMITER_CATEGORIES not set")}\'')
print(f'Initializing Limiter with AUTH_URI env. var.: \'{os.environ.get("AUTH_URI", "AUTH_URI not set")}\'')
print(f'Initializing Limiter with SKELETON_CACHE_LOW_PRIORITY_EXCHANGE env. var.: \'{os.environ.get("SKELETON_CACHE_LOW_PRIORITY_EXCHANGE", "SKELETON_CACHE_LOW_PRIORITY_EXCHANGE not set")}\'')

limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
