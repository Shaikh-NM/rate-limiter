package factory;

public class RateLimiterFactory {
    public static RateLimiterStrategy getRateLimiter(RateLimiterType type) {
        switch (type) {
            case FIXED_WINDOW:
                return new FixedWindowRateLimiter(5, 10_000);
            case SLIDING_WINDOW_LOG:
                return new SlidingWindowLogRateLimiter(5, 10_000);
            case SLIDING_WINDOW_COUNTER:
                return new SlidingWindowCounterRateLimiter(5, 10_000);
            case TOKEN_BUCKET:
                return new TokenBucketRateLimiter(5, 1);
            case LEAKY_BUCKET:
                return new LeakyBucketRateLimiter(5, 1);
            default:
                throw new IllegalArgumentException("Unsupported rate limiter type: " + type);
        }
    }
}