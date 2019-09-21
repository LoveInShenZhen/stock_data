from ratelimiter import RateLimiter

ts_rate_limiter = RateLimiter(max_calls = 2, period = 1.5)
