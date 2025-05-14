"""
As opposed to conventional network rate limiters that operate at the outer boundary of the network,
this rate limiter operates inside the Flask context. It inspects the request object and its data,
combining it with the auth-id of the user making the request, to expedite, impede, or discard requests.

Reference to a hierarchy of rate limiting policies:
https://medium.com/@SaiRahulAkarapu/rate-limiting-algorithms-using-redis-eb4427b47e33
- Fixed Window
- Sliding Logs
- Leaky Bucket
- Sliding Window
- Token Bucket
"""

import redis
from flask import g

class RateLimiter:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis = redis.StrictRedis.from_url(redis_url, decode_responses=True)
        self.default_limit = 1#1000  # Default number of tokens
        self.time_window = 60  # Time window in seconds

    def _get_user_key(self):
        """
        Generate a Redis key for the user based on their auth_id.
        """
        auth_id = g.get("auth_user", {}).get("id")
        if not auth_id:
            raise ValueError("auth_id is missing from the request context.")
        return f"rate_limiter:{auth_id}"

    def _initialize_bucket(self, user_key):
        """
        Initialize the token bucket for a user if it doesn't already exist.
        """
        if not self.redis.exists(user_key):
            self.redis.set(user_key, self.default_limit)
            self.redis.expire(user_key, self.time_window)

    def _rate_limit_exceeded_response(self):
        """
        Return a 429 Too Many Requests response when the rate limit is exceeded.
        """
        return (
            "Rate limit exceeded. Please try again later.",
            429,
            {"Content-Type": "text/plain"},
        )

    def check_rate_limit(self):
        """
        Check if the user has enough tokens to proceed with the request.
        Returns False if the request is not limited (if it is allowed), or a 429 response if the rate limit is exceeded.
        """
        user_key = self._get_user_key()
        self._initialize_bucket(user_key)

        # Use Redis' atomic decrement operation
        tokens = self.redis.decr(user_key)
        if tokens < 0:
            # If no tokens are available, reject the request
            self.redis.incr(user_key)  # Revert the decrement
            return self._rate_limit_exceeded_response()

        # If tokens are available, allow the request
        return False

    def add_tokens(self, user_key, tokens_to_add):
        """
        Add tokens to the user's bucket, ensuring it doesn't exceed the default limit.
        """
        current_tokens = int(self.redis.get(user_key) or 0)
        new_token_count = min(current_tokens + tokens_to_add, self.default_limit)
        self.redis.set(user_key, new_token_count)

    def refill_tokens(self):
        """
        Periodically refill tokens for all users. This can be implemented as a background job.
        """
        # This method is optional and can be implemented using a cron job or a task queue.
        pass
