package org.lld.ratelimiter.model;

import org.lld.ratelimiter.interfaces.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FixedWindowCounter implements RateLimiter {
    private final int maxCapacity;
    private final long windowSize;
    private final Map<String, WindowState> clients;

    public FixedWindowCounter(int maxCapacity, long windowSize){
        if(maxCapacity <= 0 || windowSize <= 0){
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
        long currentTime = System.currentTimeMillis();
        long windowSizeMs = windowSize * 1000;

        WindowState state = clients.computeIfAbsent(clientId, k -> new WindowState(currentTime));

        synchronized (state){
            long elapsedMillis = currentTime - state.getLastWindow();

            if (elapsedMillis >= windowSizeMs) {
                long windowsElapsed = elapsedMillis / windowSizeMs;
                long newWindowStart =
                        state.getLastWindow() + (windowsElapsed * windowSizeMs);

                state.setLastWindow(newWindowStart);
                state.setRequestsCount(0);
            }

            int currentCount = state.getRequestsCount();

            if (currentCount < maxCapacity) {
                state.setRequestsCount(state.getRequestsCount() + 1);
                return true;
            }

            return false;
        }
    }
}
