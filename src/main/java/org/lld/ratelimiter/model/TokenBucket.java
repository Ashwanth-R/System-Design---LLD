
package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TokenBucket implements RateLimiter {

    private final int maxTokens;
    private final int refillRate;

    private final Map<String, BucketState> clients;

    public TokenBucket(int maxTokens, int refillRate) {

        if (maxTokens <= 0 || refillRate <= 0) {
            throw new IllegalArgumentException(
                    "maxTokens and refillRate must be positive"
            );
        }

        this.maxTokens = maxTokens;
        this.refillRate = refillRate;

        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isRequestAllowed(String clientId) {

        long currentTime = System.nanoTime();

        BucketState state = clients.computeIfAbsent(
                clientId,
                id -> new BucketState(maxTokens, currentTime)
        );

        synchronized (state) {

            long elapsedTime = currentTime - state.getLastRefillTime();
            long tokensToFill = (elapsedTime * refillRate) / 1_000_000_000L;

            if (tokensToFill > 0) {
                int updatedTokens = (int) Math.min(
                        maxTokens,
                        state.getCurrentTokens() + tokensToFill
                );

                state.setCurrentTokens(updatedTokens);
                long timeConsumed = tokensToFill * 1_000_000_000L / refillRate;
                state.setLastRefillTime(
                        state.getLastRefillTime() + timeConsumed
                );
            }

            if (state.getCurrentTokens() > 0) {
                state.setCurrentTokens(
                        state.getCurrentTokens() - 1
                );
                return true;
            }
            return false;
        }
    }
}
