from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import g
import os
import json


def limit_by_category(category):
    limit = get_rate_limit_from_config(category)
    if limit is not None:
        return limiter.limit(limit, key_func=lambda: g.auth_user["id"])
    return lambda x: x


def get_rate_limit_from_config(category=None):
    if category:
        categories = json.loads(os.environ.get("LIMITER_CATEGORIES", "{}"))
        if not categories:
            # None, "", {} : The environment variable was probably populated with an empty string during deployment
            # such that it isn't literally "None", but JSON won't read an empty string, so it's just as bad as None.
            return None
        if category not in categories:
            return None  # Default rate limit if not found
        return categories[category]
    else:
        return None


limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
