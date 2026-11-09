package org.lld.ratelimiter.model;

public class BucketState {

    private int currentTokens;
    private long lastRefillTime;

    public BucketState(int maxTokens, long currentTime) {
        this.currentTokens = maxTokens;
        this.lastRefillTime = currentTime;
    }

    public int getCurrentTokens() {
        return currentTokens;
    }

    public void setCurrentTokens(int currentTokens) {
        this.currentTokens = currentTokens;
    }

    public long getLastRefillTime() {
        return lastRefillTime;
    }

    public void setLastRefillTime(long lastRefillTime) {
        this.lastRefillTime = lastRefillTime;
    }
}
