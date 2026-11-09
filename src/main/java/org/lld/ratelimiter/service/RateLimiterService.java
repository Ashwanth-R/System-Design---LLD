
package org.lld.ratelimiter.service;

import org.lld.ratelimiter.enums.RateLimiterType;
import org.lld.ratelimiter.factory.RateLimiterFactory;
import org.lld.ratelimiter.interfaces.RateLimiter;

public class RateLimiterService {

    private final RateLimiter rateLimiter;

    public RateLimiterService(
            RateLimiterType type,
            int capacity,
            int rate) {

        this.rateLimiter = RateLimiterFactory.createRateLimiter(
                type,
                capacity,
                rate
        );
    }

    public boolean isRequestAllowed(String clientId) {
        return rateLimiter.isRequestAllowed(clientId);
    }
}
