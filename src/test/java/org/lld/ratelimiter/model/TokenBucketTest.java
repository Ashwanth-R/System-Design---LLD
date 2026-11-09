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

class TokenBucketTest {
    private static final int MAX_TOKENS = 5;
    private static final int REFILL_RATE = 2; // tokens per second
    private static final String CLIENT = "user-123";

    private RateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new TokenBucket(MAX_TOKENS, REFILL_RATE);
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
    @DisplayName("Burst")
    class Burst {
        @Test
        @DisplayName("first request of a new client is allowed")
        void firstRequestAllowed() {
            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("allows up to max capacity in a burst")
        void allowsUpToCapacity() {
            for (int i = 1; i <= MAX_TOKENS; i++) {
                assertTrue(rateLimiter.isRequestAllowed(CLIENT), "request " + i + " should be allowed");
            }
        }

        @Test
        @DisplayName("rejects requests once the bucket is empty")
        void rejectsAfterCapacity() {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);

            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("burst of 10 -> exactly 5 allowed, 5 rejected")
        void burstOfTen() {
            assertEquals(MAX_TOKENS, sendRequests(rateLimiter, CLIENT, 10));
        }

        @Test
        @DisplayName("each client has its own bucket")
        void clientsAreIndependent() {
            sendRequests(rateLimiter, "client-A", MAX_TOKENS);
            assertFalse(rateLimiter.isRequestAllowed("client-A"));

            assertEquals(MAX_TOKENS, sendRequests(rateLimiter, "client-B", MAX_TOKENS));
        }
    }

    @Nested
    @DisplayName("Refill")
    class Refill {
        @Test
        @DisplayName("no tokens are added before a full token's worth of time passes")
        void noRefillTooEarly() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);

            Thread.sleep(300); // < 500ms needed for 1 token at 2/sec

            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("after 1 sec, exactly refillRate (2) tokens are added")
        void refillsAfterOneSecond() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));

            Thread.sleep(1100);

            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("refill never exceeds max capacity")
        void refillCappedAtCapacity() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);

            Thread.sleep(3100); // would be 6 tokens, but capacity is 5

            assertEquals(MAX_TOKENS, sendRequests(rateLimiter, CLIENT, MAX_TOKENS + 3));
        }

        @Test
        @DisplayName("idle client with full bucket does not go above capacity")
        void fullBucketStaysAtCapacity() throws InterruptedException {
            rateLimiter.isRequestAllowed(CLIENT); // create bucket, 4 left

            Thread.sleep(2100);

            assertEquals(MAX_TOKENS, sendRequests(rateLimiter, CLIENT, MAX_TOKENS + 3));
        }

        @Test
        @DisplayName("leftover time past a refill is carried over, not discarded")
        void partialTimeCarriedOver() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);

            Thread.sleep(1300); // 1000ms -> 2 tokens (1 per 500ms), 300ms left over
            assertEquals(2, sendRequests(rateLimiter, CLIENT, MAX_TOKENS));

            Thread.sleep(300);  // 300 left over + 300 = 600ms -> 1 token
            // if leftover were discarded, only 300ms would count -> 0 tokens -> rejected
            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("tokens refill gradually: 1 token every 500ms at 2 tokens/sec")
        void subSecondRefill() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);

            Thread.sleep(600); // enough time for 1 token

            assertTrue(rateLimiter.isRequestAllowed(CLIENT));
            assertFalse(rateLimiter.isRequestAllowed(CLIENT));
        }

        @Test
        @DisplayName("rejected -> refill -> allowed -> rejected again")
        void rejectedThenAllowedAfterRefill() throws InterruptedException {
            sendRequests(rateLimiter, CLIENT, MAX_TOKENS);
            assertFalse(rateLimiter.isRequestAllowed(CLIENT), "bucket empty");

            Thread.sleep(1100);
            assertEquals(REFILL_RATE, sendRequests(rateLimiter, CLIENT, MAX_TOKENS), "2 refilled");

            Thread.sleep(1100);
            assertEquals(REFILL_RATE, sendRequests(rateLimiter, CLIENT, MAX_TOKENS), "2 refilled again");
        }
    }

    @Nested
    @DisplayName("Concurrency / thread safety")
    class Concurrency {
        // Low refill rate + big capacity so no refill sneaks in while the threads run
        private static final int CAPACITY = 50;
        private static final int SLOW_REFILL = 1;

        @Test
        @DisplayName("many threads, same client -> exactly capacity allowed (no over-granting)")
        void concurrentRequestsSameClient() throws Exception {
            RateLimiter limiter = new TokenBucket(CAPACITY, SLOW_REFILL);
            int threads = 200;

            AtomicInteger allowed = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            runConcurrently(threads, () -> {
                if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
                else rejected.incrementAndGet();
            });

            assertEquals(CAPACITY, allowed.get());
            assertEquals(threads - CAPACITY, rejected.get());
        }

        @Test
        @DisplayName("many threads, many clients -> each client gets exactly its capacity")
        void concurrentRequestsManyClients() throws Exception {
            RateLimiter limiter = new TokenBucket(MAX_TOKENS, SLOW_REFILL);
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
                    assertEquals(MAX_TOKENS, count.get(), clientId + " allowed count"));
        }

        @Test
        @DisplayName("first-ever concurrent requests initialise the bucket only once")
        void concurrentFirstAccessInitialisesOnce() throws Exception {
            // repeat with fresh limiters to give races on putIfAbsent a chance to show up
            for (int run = 0; run < 50; run++) {
                RateLimiter limiter = new TokenBucket(MAX_TOKENS, SLOW_REFILL);
                AtomicInteger allowed = new AtomicInteger();

                runConcurrently(20, () -> {
                    if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
                });

                assertEquals(MAX_TOKENS, allowed.get(), "run " + run);
            }
        }

        @Test
        @DisplayName("concurrent requests after a refill grant only the refilled tokens")
        void concurrentRequestsAfterRefill() throws Exception {
            RateLimiter limiter = new TokenBucket(MAX_TOKENS, REFILL_RATE);
            sendRequests(limiter, CLIENT, MAX_TOKENS);

            Thread.sleep(1100);

            AtomicInteger allowed = new AtomicInteger();
            runConcurrently(50, () -> {
                if (limiter.isRequestAllowed(CLIENT)) allowed.incrementAndGet();
            });

            assertEquals(REFILL_RATE, allowed.get());
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
