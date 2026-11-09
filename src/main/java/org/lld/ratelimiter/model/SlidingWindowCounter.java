package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlidingWindowCounter implements RateLimiter {
    private final int maxCapacity;
    private final long windowSize;
    private final Map<String, SlidingWindowState> clients;

    public SlidingWindowCounter(int maxCapacity, int windowSize){
        if(maxCapacity <=0 || windowSize <= 0){
            throw new IllegalArgumentException(
                    "maxCapacity and windowSize must be positive"
            );
        }
        this.maxCapacity = maxCapacity;
        this.windowSize = windowSize;
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isRequestAllowed(String clientId){

        SlidingWindowState state = clients.computeIfAbsent(
                clientId,
                k -> new SlidingWindowState()
        );

        synchronized (state){
            long currentTime = System.currentTimeMillis();
            long windowSizeMs = windowSize * 1000;
            long elapsedTimeMs = currentTime - state.getCurrentWindowStartTime();

            while(elapsedTimeMs >= windowSizeMs){
                state.setPreviousCounter(state.getCurrentCounter());
                state.setCurrentCounter(0);
                state.setCurrentWindowStartTime(state.getCurrentWindowStartTime() + windowSizeMs);
                elapsedTimeMs = currentTime - state.getCurrentWindowStartTime();
            }

            double currentWindowProgress = (double) elapsedTimeMs / windowSizeMs;
            double previousWindowsProgress = 1.0 - currentWindowProgress;

            double weightedCount = state.getPreviousCounter() * previousWindowsProgress + state.getCurrentCounter();

            if(weightedCount + 1 < maxCapacity){
                state.setCurrentCounter(state.getCurrentCounter() + 1);
                return true;
            }
            return false;
        }
    }
}
