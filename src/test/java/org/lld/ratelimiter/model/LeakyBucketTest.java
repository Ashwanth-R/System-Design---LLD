package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LeakyBucketTest {
    private static final int CAPACITY = 5;
    private static final int LEAK_RATE = 2; // requests leaked per second -> 1 every 500ms
    private static final String CLIENT = "user-123";

    private RateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new LeakyBucket(CAPACITY, LEAK_RATE);
    }

    /** Sends {@code count} requests back to back and returns how many were allowed. */
    private int sendRequests(RateLimiter limiter, String clientId, int count) {
        int allowed = 0;
        for (int i = 0; i < count; i++) {
            if (limiter.isRequestAllowed(clientId)) allowed++;
        }
        return allowed;
    }

    @Nested
    @DisplayName("Constructor validation")
    class Validation {
        @Test
        @DisplayName("negative capacity or leak rate throws")
        void negativeArgsThrow() {
            assertThrows(IllegalArgumentException.class, () -> new LeakyBucket(-1, LEAK_RATE));
            assertThrows(IllegalArgumentException.class, () -> new LeakyBucket(CAPACITY, -1));
        }

        @Test
        @DisplayName("zero capacity or leak rate throws (must be positive)")
        void zeroArgsThrow() {
            assertThrows(IllegalArgumentException.class, () -> new LeakyBucket(0, LEAK_RATE));
            assertThrows(IllegalArgumentException.class, () -> new LeakyBucket(CAPACITY, 0));
        }
    }

    @Nested
    @DisplayName("Burst")
    class Burst {
        @Test
        @DisplayName("first request of a new client is allowed")
        void firstRequestAllowed() {
            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("allows up to capacity, then rejects when the bucket is full")
        void fillsToCapacityThenRejects() {
            for (int i = 1; i <= CAPACITY; i++) {
                assertTrue(rateLimiter.isRequestAllowed(CLIENT), "request " + i + " should be allowed");
            }
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("burst of 10 -> exactly 5 allowed, 5 rejected")
        void burstOfTen() {
            assertEquals(CAPACITY, sendRequests(rateLimiter, CLIENT, 10));
        }

        @Test
        @DisplayName("each client has its own bucket")
        void clientsAreIndependent() {
            sendRequests(rateLimiter, "client-A", CAPACITY);
            assertFalse(rateLimiter.isRequestAllowed("client-A"));

            assertEquals(CAPACITY, sendRequests(rateLimiter, "client-B", CAPACITY));
        }
    }

    @Nested
    @DisplayName("Leak")
    class Leak {
        @Test
        @DisplayName("nothing leaks before one request's worth of time (500ms)")
        void noLeakTooEarly() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(300);

            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("after 600ms, exactly 1 request has leaked -> 1 slot free")
        void leaksOneAfterHalfSecond() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(600);

            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("after 1.1s, exactly leakRate (2) requests have leaked")
        void leaksTwoAfterOneSecond() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(1100);

            assertEquals(LEAK_RATE, sendRequests(rateLimiter, CLIENT, CAPACITY));
        }

        @Test
        @DisplayName("long idle fully drains the bucket, but never beyond capacity")
        void fullyDrainsAfterLongIdle() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(3100); // would leak 6, but only 5 are queued

            assertEquals(CAPACITY, sendRequests(rateLimiter, CLIENT, CAPACITY + 3));
        }

        @Test
        @DisplayName("keeps leaking across repeated partial drains")
        void keepsLeakingAfterPartialDrain() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(1100);
            assertEquals(2, sendRequests(rateLimiter, CLIENT, CAPACITY), "first leak");

            Thread.sleep(1100);
            assertEquals(2, sendRequests(rateLimiter, CLIENT, CAPACITY), "second leak");

            Thread.sleep(1100);
            assertEquals(2, sendRequests(rateLimiter, CLIENT, CAPACITY), "third leak");
        }

        @Test
        @DisplayName("leftover time past a leak is carried over, not discarded")
        void partialTimeCarriedOver() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            Thread.sleep(1300); // 1000ms -> 2 leaked, 300ms left over
            assertEquals(2, sendRequests(rateLimiter, CLIENT, CAPACITY));

            Thread.sleep(300);  // 300 left over + 300 = 600ms -> 1 more leaked
            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("steady traffic at the leak rate is always allowed once full")
        void steadyTrafficAtLeakRate() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, CAPACITY);

            for (int i = 1; i <= 4; i++) {
                Thread.sleep(550);
                assertTrue(rateLimiter.isRequestAllowed(CLIENT), "steady request " + i);
            }
        }
    }

    @Nested
    @DisplayName("Concurrency / thread safety")
    class Concurrency {
        // Low leak rate + big capacity so nothing leaks while the threads run
        private static final int BIG_CAPACITY = 50;
        private static final int SLOW_LEAK = 1;

        @Test
        @DisplayName("many threads, same client -> exactly capacity allowed (no over-filling)")
        void concurrentRequestsSameClient() throws Exception {
            RateLimiter limiter = new LeakyBucket(BIG_CAPACITY, SLOW_LEAK);
            int threads = 200;

            AtomicInteger allowed = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            runConcurrently(threads, () -> {
                if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
                else rejected.incrementAndGet();
            });

            assertEquals(BIG_CAPACITY, allowed.get());
            assertEquals(threads - BIG_CAPACITY, rejected.get());
        }

        @Test
        @DisplayName("many threads, many clients -> each client gets exactly its capacity")
        void concurrentRequestsManyClients() throws Exception {
            RateLimiter limiter = new LeakyBucket(CAPACITY, SLOW_LEAK);
            int clients = 20;
            int requestsPerClient = 30;

            Map<String, AtomicInteger> allowedPerClient = new ConcurrentHashMap<>();
            for (int c = 0; c < clients; c++) allowedPerClient.put("client-" + c, new AtomicInteger());

            List<Runnable> tasks = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                String clientId = "client-" + c;
                for (int r = 0; r < requestsPerClient; r++) {
                    tasks.add(() -> {
                        if (limiter.isRequestAllowed(clientId)) allowedPerClient.get(clientId).incrementAndGet();
                    });
                }
            }
            runConcurrently(tasks);

            allowedPerClient.forEach((clientId, count) ->
                    assertEquals(CAPACITY, count.get(), clientId + " allowed count"));
        }

        @Test
        @DisplayName("first-ever concurrent requests create the queue only once")
        void concurrentFirstAccessInitialisesOnce() throws Exception {
            for (int run = 0; run < 50; run++) {
                RateLimiter limiter = new LeakyBucket(CAPACITY, SLOW_LEAK);
                AtomicInteger allowed = new AtomicInteger();

                runConcurrently(20, () -> {
                    if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
                });

                assertEquals(CAPACITY, allowed.get(), "run " + run);
            }
        }

        @Test
        @DisplayName("concurrent requests after a leak fill only the freed slots")
        void concurrentRequestsAfterLeak() throws Exception {
            RateLimiter limiter = new LeakyBucket(CAPACITY, LEAK_RATE);
            sendRequests(limiter, CLIENT, CAPACITY);

            Thread.sleep(1100);

            AtomicInteger allowed = new AtomicInteger();
            runConcurrently(50, () -> {
                if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
            });

            assertEquals(LEAK_RATE, allowed.get());
        }

        private void runConcurrently(int threads, Runnable task) throws Exception {
            List<Runnable> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) tasks.add(task);
            runConcurrently(tasks);
        }

        /** Starts all tasks at the same instant (via a latch) to maximise contention. */
        private void runConcurrently(List<Runnable> tasks) throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(Math.min(tasks.size(), 64));
            CountDownLatch ready = new CountDownLatch(1);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (Runnable task : tasks) {
                    futures.add(executor.submit(() -> {
                        ready.await();
                        task.run();
                        return null;
                    }));
                }
                ready.countDown();
                for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS); // rethrows any failure
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
