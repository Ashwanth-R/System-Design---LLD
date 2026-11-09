package org.lld.ratelimiter.model;

public class WindowState {
    private int requestsCount;
    private long lastWindow;

    public WindowState(long currentTime){
        this.requestsCount = 0;
        this.lastWindow = currentTime;
    }


    public int getRequestsCount() {
        return requestsCount;
    }

    public void setRequestsCount(int requestsCount) {
        this.requestsCount = requestsCount;
    }

    public long getLastWindow() {
        return lastWindow;
    }

    public void setLastWindow(long lastWindow) {
        this.lastWindow = lastWindow;
    }
}
