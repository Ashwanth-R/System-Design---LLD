
package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeakyBucket implements RateLimiter {

    private final int maxCapacity;
    private final int leakRate;

    private final Map<String, LeakyBucketState> clients;

    public LeakyBucket(int maxCapacity, int leakRate) {

        if (maxCapacity <= 0 || leakRate <= 0) {
            throw new IllegalArgumentException(
                    "maxCapacity and leakRate must be positive"
            );
        }

        this.maxCapacity = maxCapacity;
        this.leakRate = leakRate;

        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isRequestAllowed(String clientId) {

        long currentTime = System.nanoTime();

        LeakyBucketState state = clients.computeIfAbsent(
                clientId,
                id -> new LeakyBucketState(currentTime)
        );

        synchronized (state) {
            long lastLeakTime = state.getLastLeakTime();
            long elapsedTime = currentTime - lastLeakTime;

            long requestsToLeak =
                    (elapsedTime * leakRate) / 1_000_000_000L;

            var queue = state.getRequestsQueue();

            long actualRequestsLeaked = Math.min(
                    requestsToLeak,
                    queue.size()
            );

            // Remove leaked requests
            for (int i = 0; i < actualRequestsLeaked; i++) {
                Long removedTimestamp = queue.poll();
                System.out.println(
                        "Client: " + clientId +
                                " | Request timestamp removed: " + removedTimestamp
                );
            }

            // Update leak timestamp
            if (queue.isEmpty()) {
                state.setLastLeakTime(currentTime);
            } else if (requestsToLeak > 0) {
                long timeConsumed =
                        actualRequestsLeaked * 1_000_000_000L / leakRate;
                state.setLastLeakTime(
                        lastLeakTime + timeConsumed
                );
            }

            // Check bucket capacity
            if (queue.size() < maxCapacity) {
                queue.offer(currentTime);
                return true;
            }
            return false;
        }
    }
}
