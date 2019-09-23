from ratelimiter import RateLimiter

ts_rate_limiter = RateLimiter(max_calls = 1, period = 1)
