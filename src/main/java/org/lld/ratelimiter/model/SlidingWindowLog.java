package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlidingWindowLog implements RateLimiter {
    private final int maxCapacity;
    private final long windowSize;
    private final Map<String, Deque<Long>> clients;

    public SlidingWindowLog(int maxCapacity, long windowSize){
        if(maxCapacity <=0 || windowSize <= 0){
            throw new IllegalArgumentException(
                    "maxCapacity and windowsSize must be positive"
            );
        }
        this.maxCapacity = maxCapacity;
        this.windowSize = windowSize;
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isRequestAllowed(String clientId){
        long currentTime = System.currentTimeMillis();
        long windowSizeMs = windowSize * 1000;

        clients.computeIfAbsent(clientId, k -> new ArrayDeque<>());

        long windowStartTime = currentTime - windowSizeMs;

        Deque<Long> clientRequests = clients.get(clientId);

        synchronized (clientRequests){
            while(!clientRequests.isEmpty() && clientRequests.peek() < windowStartTime){
                clientRequests.poll();
            }

            if(clientRequests.size() < maxCapacity){
                clientRequests.offer(currentTime);
                return true;
            }
            return false;
        }
    }
}
