package org.lld.ratelimiter;

import org.lld.ratelimiter.enums.RateLimiterType;
import org.lld.ratelimiter.service.RateLimiterService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        RateLimiterService service = new RateLimiterService(
                RateLimiterType.TOKEN_BUCKET,
                5,
                2
        );

        ExecutorService executor = Executors.newFixedThreadPool(5);

        // Client A - 3 threads
        for (int i = 1; i <= 3; i++) {

            int threadId = i;

            executor.submit(() -> {

                for (int j = 1; j <= 5; j++) {

                    boolean allowed =
                            service.isRequestAllowed("CLIENT_A");

                    System.out.println(
                            "Thread: A-" + threadId +
                                    " | Client: CLIENT_A" +
                                    " | Request: " + j +
                                    " | Allowed: " + allowed
                    );
                }
            });
        }

        // Client B - 2 threads
        for (int i = 1; i <= 2; i++) {

            int threadId = i;

            executor.submit(() -> {

                for (int j = 1; j <= 5; j++) {

                    boolean allowed =
                            service.isRequestAllowed("CLIENT_B");

                    System.out.println(
                            "Thread: B-" + threadId +
                                    " | Client: CLIENT_B" +
                                    " | Request: " + j +
                                    " | Allowed: " + allowed
                    );
                }
            });
        }

        executor.shutdown();

        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        System.out.println("All requests completed!");
    }
}
