package org.lld.ratelimiter.model;

import java.util.ArrayDeque;
import java.util.Deque;

public class LeakyBucketState {

    private final Deque<Long> requestsQueue;
    private long lastLeakTime;

    public LeakyBucketState(long currentTime) {
        this.requestsQueue = new ArrayDeque<>();
        this.lastLeakTime = currentTime;
    }

    public Deque<Long> getRequestsQueue() {
        return requestsQueue;
    }

    public long getLastLeakTime() {
        return lastLeakTime;
    }

    public void setLastLeakTime(long lastLeakTime) {
        this.lastLeakTime = lastLeakTime;
    }
}