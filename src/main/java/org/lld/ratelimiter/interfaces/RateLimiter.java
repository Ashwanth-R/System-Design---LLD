package org.lld.ratelimiter.interfaces;

public interface RateLimiter {
    public boolean isRequestAllowed(String clientId);
}
