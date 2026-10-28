import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    // ==========================================
    // 1. Enum
    // ==========================================
    public enum RateLimiterType {
        FIXED_WINDOW,
        SLIDING_WINDOW_LOG,
        SLIDING_WINDOW_COUNTER,
        TOKEN_BUCKET,
        LEAKY_BUCKET
    }

    // ==========================================
    // 2. Strategy Interface
    // ==========================================
    public interface RateLimiterStrategy {
        boolean allowRequest(String userId);
        String getName();
    }

    // ==========================================
    // 3. Strategy Implementations
    // ==========================================

    // 3.1 Token Bucket
    public static class TokenBucketRateLimiter implements RateLimiterStrategy {
        private final long capacity;
        private final double refillRatePerSecond;
        private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

        public TokenBucketRateLimiter(long capacity, double refillRatePerSecond) {
            this.capacity = capacity;
            this.refillRatePerSecond = refillRatePerSecond;
        }

        @Override
        public boolean allowRequest(String userId) {
            return buckets.computeIfAbsent(userId, k -> new Bucket(capacity, refillRatePerSecond))
                    .tryConsume();
        }

        @Override
        public String getName() {
            return "Token Bucket";
        }

        private static class Bucket {
            private final long capacity;
            private final double refillRatePerMs;
            private double tokens;
            private final AtomicLong lastRefillTime;

            public Bucket(long capacity, double refillRatePerSecond) {
                this.capacity = capacity;
                this.refillRatePerMs = refillRatePerSecond / 1000.0;
                this.tokens = capacity;
                this.lastRefillTime = new AtomicLong(System.currentTimeMillis());
            }

            public synchronized boolean tryConsume() {
                refill();
                if (tokens >= 1.0) {
                    tokens -= 1.0;
                    return true;
                }
                return false;
            }

            private void refill() {
                long now = System.currentTimeMillis();
                long elapsed = now - lastRefillTime.get();
                if (elapsed > 0) {
                    tokens = Math.min(capacity, tokens + (elapsed * refillRatePerMs));
                    lastRefillTime.set(now);
                }
            }
        }
    }

    // 3.2 Fixed Window
    public static class FixedWindowRateLimiter implements RateLimiterStrategy {
        private final int limit;
        private final long windowSizeMs;
        private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

        public FixedWindowRateLimiter(int limit, long windowSizeMs) {
            this.limit = limit;
            this.windowSizeMs = windowSizeMs;
        }

        @Override
        public boolean allowRequest(String userId) {
            long currentWindow = System.currentTimeMillis() / windowSizeMs;
            Window window = windows.compute(userId, (k, existing) -> {
                if (existing == null || existing.windowKey != currentWindow) {
                    return new Window(currentWindow);
                }
                return existing;
            });
            return window.counter.incrementAndGet() <= limit;
        }

        @Override
        public String getName() {
            return "Fixed Window";
        }

        private static class Window {
            final long windowKey;
            final AtomicInteger counter = new AtomicInteger(0);

            Window(long windowKey) {
                this.windowKey = windowKey;
            }
        }
    }

    // 3.3 Sliding Window Log
    public static class SlidingWindowLogRateLimiter implements RateLimiterStrategy {
        private final int limit;
        private final long windowSizeMs;
        private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> logs = new ConcurrentHashMap<>();

        public SlidingWindowLogRateLimiter(int limit, long windowSizeMs) {
            this.limit = limit;
            this.windowSizeMs = windowSizeMs;
        }

        @Override
        public boolean allowRequest(String userId) {
            long now = System.currentTimeMillis();
            long boundary = now - windowSizeMs;
            ConcurrentLinkedQueue<Long> queue = logs.computeIfAbsent(userId, k -> new ConcurrentLinkedQueue<>());

            synchronized (queue) {
                while (!queue.isEmpty() && queue.peek() <= boundary) {
                    queue.poll();
                }
                if (queue.size() < limit) {
                    queue.offer(now);
                    return true;
                }
                return false;
            }
        }

        @Override
        public String getName() {
            return "Sliding Window Log";
        }
    }

    // 3.4 Sliding Window Counter
    public static class SlidingWindowCounterRateLimiter implements RateLimiterStrategy {
        private final int limit;
        private final long windowSizeMs;
        private final ConcurrentHashMap<String, UserWindow> userWindows = new ConcurrentHashMap<>();

        public SlidingWindowCounterRateLimiter(int limit, long windowSizeMs) {
            this.limit = limit;
            this.windowSizeMs = windowSizeMs;
        }

        @Override
        public synchronized boolean allowRequest(String userId) {
            long now = System.currentTimeMillis();
            UserWindow userWindow = userWindows.computeIfAbsent(userId, k -> new UserWindow(now));

            long currentWindowStart = (now / windowSizeMs) * windowSizeMs;
            if (userWindow.currentWindowStart != currentWindowStart) {
                if (userWindow.currentWindowStart == currentWindowStart - windowSizeMs) {
                    userWindow.previousCount = userWindow.currentCount;
                } else {
                    userWindow.previousCount = 0;
                }
                userWindow.currentCount = 0;
                userWindow.currentWindowStart = currentWindowStart;
            }

            double timeIntoCurrentWindow = (double) (now - currentWindowStart);
            double previousWeight = 1.0 - (timeIntoCurrentWindow / windowSizeMs);
            double estimatedRequests = (userWindow.previousCount * previousWeight) + userWindow.currentCount;

            if (estimatedRequests < limit) {
                userWindow.currentCount++;
                return true;
            }
            return false;
        }

        @Override
        public String getName() {
            return "Sliding Window Counter";
        }

        private static class UserWindow {
            long currentWindowStart;
            int previousCount = 0;
            int currentCount = 0;

            UserWindow(long now) {
                this.currentWindowStart = now;
            }
        }
    }

    // 3.5 Leaky Bucket
    public static class LeakyBucketRateLimiter implements RateLimiterStrategy {
        private final long capacity;
        private final double leakRatePerSecond;
        private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

        public LeakyBucketRateLimiter(long capacity, double leakRatePerSecond) {
            this.capacity = capacity;
            this.leakRatePerSecond = leakRatePerSecond;
        }

        @Override
        public boolean allowRequest(String userId) {
            return buckets.computeIfAbsent(userId, k -> new Bucket(capacity, leakRatePerSecond))
                    .tryAdd();
        }

        @Override
        public String getName() {
            return "Leaky Bucket";
        }

        private static class Bucket {
            private final long capacity;
            private final double leakRatePerMs;
            private double waterLevel = 0.0;
            private final AtomicLong lastLeakTime;

            public Bucket(long capacity, double leakRatePerSecond) {
                this.capacity = capacity;
                this.leakRatePerMs = leakRatePerSecond / 1000.0;
                this.lastLeakTime = new AtomicLong(System.currentTimeMillis());
            }

            public synchronized boolean tryAdd() {
                leak();
                if (waterLevel < capacity) {
                    waterLevel += 1.0;
                    return true;
                }
                return false;
            }

            private void leak() {
                long now = System.currentTimeMillis();
                long elapsed = now - lastLeakTime.get();
                if (elapsed > 0) {
                    double leaked = elapsed * leakRatePerMs;
                    waterLevel = Math.max(0.0, waterLevel - leaked);
                    lastLeakTime.set(now);
                }
            }
        }
    }

    // ==========================================
    // 4. Factory / Strategy Selector
    // ==========================================
    public static class RateLimiterFactory {
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
                    throw new IllegalArgumentException("Unsupported rate limiter type");
            }
        }
    }

    // ==========================================
    // 5. Service using Strategy (ApiGateway)
    // ==========================================
    public static class ApiGateway {
        private volatile RateLimiterStrategy strategy;

        public ApiGateway(RateLimiterStrategy strategy) {
            this.strategy = strategy;
        }

        // Allows runtime strategy swap
        public void setStrategy(RateLimiterStrategy strategy) {
            this.strategy = strategy;
        }

        public void handleRequest(String userId, int requestId) {
            boolean allowed = strategy.allowRequest(userId);
            if (allowed) {
                System.out.println("Request " + requestId + ": ALLOWED");
            } else {
                System.out.println("Request " + requestId + ": REJECTED (Rate limit exceeded)");
            }
        }
    }

    // ==========================================
    // 6. Main Runner
    // ==========================================
    public static void main(String[] args) throws InterruptedException {
        /*
         * Imagine this value comes from config:
         * application.yml
         * rate-limiter:
         *   algorithm: TOKEN_BUCKET
         */
        RateLimiterType selectedAlgorithm = RateLimiterType.TOKEN_BUCKET;

        RateLimiterStrategy strategy = RateLimiterFactory.getRateLimiter(selectedAlgorithm);

        ApiGateway apiGateway = new ApiGateway(strategy);

        String userId = "user-123";

        System.out.println("Selected Algorithm: " + strategy.getName());
        System.out.println("-------------------------------------");

        for (int i = 1; i <= 10; i++) {
            apiGateway.handleRequest(userId, i);
            Thread.sleep(300);
        }
    }
}