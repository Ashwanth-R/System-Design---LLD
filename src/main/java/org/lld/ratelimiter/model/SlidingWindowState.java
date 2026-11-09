package org.lld.ratelimiter.model;

public class SlidingWindowState {
    private int previousCounter;
    private int currentCounter;
    private long currentWindowStartTime;

    public SlidingWindowState(){
        this.previousCounter = 0;
        this.currentCounter = 0;
        this.currentWindowStartTime = System.currentTimeMillis();
    }

    public int getPreviousCounter() {
        return previousCounter;
    }

    public void setPreviousCounter(int previousCounter) {
        this.previousCounter = previousCounter;
    }

    public long getCurrentWindowStartTime() {
        return currentWindowStartTime;
    }

    public void setCurrentWindowStartTime(long currentWindowStartTime) {
        this.currentWindowStartTime = currentWindowStartTime;
    }

    public int getCurrentCounter() {
        return currentCounter;
    }

    public void setCurrentCounter(int currentCounter) {
        this.currentCounter = currentCounter;
    }
}
