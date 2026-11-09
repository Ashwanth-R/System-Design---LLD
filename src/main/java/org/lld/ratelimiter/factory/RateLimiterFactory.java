
package org.lld.ratelimiter.factory;

import org.lld.ratelimiter.enums.RateLimiterType;
import org.lld.ratelimiter.interfaces.RateLimiter;
import org.lld.ratelimiter.model.*;

public class RateLimiterFactory {

    public static RateLimiter createRateLimiter(RateLimiterType type, int capacity, int rate) {
        return switch (type) {

            case TOKEN_BUCKET ->
                    new TokenBucket(capacity, rate);

            case LEAKY_BUCKET ->
                    new LeakyBucket(capacity, rate);

            case FIXED_WINDOW ->
                    new FixedWindowCounter(capacity, rate);

            case SLIDING_WINDOW_LOG ->
                    new SlidingWindowLog(capacity, rate);

            case SLIDING_WINDOW_COUNTER ->
                    new SlidingWindowCounter(capacity, rate);
        };
    }
}
