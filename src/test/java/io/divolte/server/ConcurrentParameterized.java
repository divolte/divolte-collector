/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
