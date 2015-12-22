/*
 * Copyright 2014 GoDataDriven B.V.
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

package io.divolte.server.processing;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.divolte.server.processing.ItemProcessor.ProcessingDirective;

@ParametersAreNonnullByDefault
public class ProcessingPool<T extends ItemProcessor<E>, E> {
    private static final Logger logger = LoggerFactory.getLogger(ProcessingPool.class);

    private static final int MAX_BATCH_SIZE = 128;

    private final ExecutorService executorService;
    private final List<BlockingQueue<Item<E>>> queues;

    private volatile boolean running;

    private final Supplier<T> processorSupplier;


    public ProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final String threadBaseName,
            final Supplier<T> processorSupplier) {

        running = true;

        this.processorSupplier = processorSupplier;

        @SuppressWarnings("PMD.AvoidThreadGroup")
        final ThreadGroup threadGroup = new ThreadGroup(threadBaseName + " group");
        final ThreadFactory factory = createThreadFactory(threadGroup, threadBaseName + " - %d");
        executorService = Executors.newFixedThreadPool(numThreads, factory);

        this.queues = Stream.<ArrayBlockingQueue<Item<E>>>
                generate(() -> new ArrayBlockingQueue<>(maxQueueSize))
                .limit(numThreads)
                .collect(Collectors.toCollection(() -> new ArrayList<>(numThreads)));

        queues.forEach((queue) -> scheduleQueueReader(
                executorService,
                queue,
                processorSupplier.get()));

    }

    public void enqueue(final Item<E> item) {
        final BlockingQueue<Item<E>> queue = queues.get(item.affinityHash % queues.size());
        if (!queue.offer(item)) {
            logger.warn("Failed to enqueue item. Dropping event.");
        }
    }

    public void stop() {
        try {
            running = false;
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleQueueReader(final ExecutorService es, final BlockingQueue<Item<E>> queue, final ItemProcessor<E> processor) {
        CompletableFuture.runAsync(microBatchingQueueDrainerWithHeartBeat(queue, processor), es).whenComplete((voidValue, error) -> {
            processor.cleanup();

            // In case the reader for some reason escapes its loop with an
            // exception, log any uncaught exceptions and reschedule
                if (error != null && running) {
                    logger.warn("Uncaught exception in incoming queue reader thread.", error);
                    scheduleQueueReader(es, queue, processorSupplier.get());
                }
            });
    }

    private Runnable microBatchingQueueDrainerWithHeartBeat(
            final BlockingQueue<Item<E>> queue,
            final ItemProcessor<E> processor) {
        return () -> {
            // The default item processor implementation removes items one-by-one as they
            // are processed. Using a Queue ensures that this is efficient.
            final Queue<Item<E>> batch = new ArrayDeque<>(MAX_BATCH_SIZE);

            while (!queue.isEmpty() || running) {
                ProcessingDirective directive;
                do {
                    queue.drainTo(batch, MAX_BATCH_SIZE - batch.size());
                    if (batch.isEmpty()) {
                        // If the batch was empty, block on the queue for some time
                        // until something is available.
                        directive = Optional.ofNullable(pollQuietly(queue, 1, TimeUnit.SECONDS))
                        .map((p) -> {
                            batch.add(p);
                            return CONTINUE;
                        })
                        .orElseGet(processor::heartbeat);
                    } else {
                        directive = processor.process(batch);
                    }
                } while (directive == CONTINUE && running);

                while (directive == PAUSE && running) {
                    sleepOneSecond();
                    directive = processor.heartbeat();
                }
            }
        };
    }

    private static void sleepOneSecond() {
        try {
            Thread.sleep(1000);
        } catch(final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <E> E pollQuietly(final BlockingQueue<E> queue, final long timeout, final TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static ThreadFactory createThreadFactory(final ThreadGroup group, final String nameFormat) {
        return new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setThreadFactory((runnable) -> new Thread(group, runnable))
            .build();
    }
}
