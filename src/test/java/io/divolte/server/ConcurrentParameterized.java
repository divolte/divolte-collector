package io.divolte.server;

import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ParametersAreNonnullByDefault
public class ConcurrentParameterized extends Parameterized {
    // This happens to the maximum number of tests we can run in parallel with Sauce Labs.
    private static final int MAXIMUM_CONCURRENCY = 5;

    public ConcurrentParameterized(final Class<?> cls) throws Throwable {
        super(cls);
        setScheduler(new ConcurrentRunner());
    }

    private static class ConcurrentRunner implements RunnerScheduler {
        private final ExecutorService executorService = Executors.newFixedThreadPool(MAXIMUM_CONCURRENCY);

        public void schedule(final Runnable childStatement) {
            executorService.submit(childStatement);
        }

        public void finished() {
            // Only stops new tasks being submitted; doesn't abort.
            executorService.shutdown();
            // Wait for everything submitted to complete.
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (final InterruptedException e) {
                // Preserve interrupted status.
                Thread.currentThread().interrupt();
            }
        }
    }
}
