package io.divolte.server.processing;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;
import io.divolte.server.processing.ItemProcessor.ProcessingDirective;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
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

@ParametersAreNonnullByDefault
public class ProcessingPool<T extends ItemProcessor<E>, E> {
    private static final Logger logger = LoggerFactory.getLogger(ProcessingPool.class);

    private static final int MAX_BATCH_SIZE = 128;

    private final ExecutorService executorService;
    private final List<BlockingQueue<E>> queues;
    private final long maxEnqueueDelay;

    private volatile boolean running;

    private final Supplier<T> processorSupplier;


    public ProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final long maxEnqueueDelay,
            final String threadBaseName,
            final Supplier<T> processorSupplier) {

        running = true;

        this.processorSupplier = processorSupplier;

        @SuppressWarnings("PMD.AvoidThreadGroup")
        final ThreadGroup threadGroup = new ThreadGroup(threadBaseName + " group");
        final ThreadFactory factory = createThreadFactory(threadGroup, threadBaseName + " - %d");
        executorService = Executors.newFixedThreadPool(numThreads, factory);

        this.maxEnqueueDelay = maxEnqueueDelay;

        this.queues = Stream.<ArrayBlockingQueue<E>>
                generate(() -> new ArrayBlockingQueue<>(maxQueueSize))
                .limit(numThreads)
                .collect(Collectors.toCollection(() -> new ArrayList<>(numThreads)));

        queues.forEach((queue) -> {
                    scheduleQueueReader(
                            executorService,
                            queue,
                            processorSupplier.get());
                });

    }

    public void enqueue(String key, E e) {
        // We mask the hash-code to ensure we always get a positive bucket index.
        if (!offerQuietly(
                queues.get((key.hashCode() & Integer.MAX_VALUE) % queues.size()),
                e,
                maxEnqueueDelay,
                TimeUnit.MILLISECONDS)) {
            logger.warn("Failed to enqueue item for {} ms. Dropping event.", maxEnqueueDelay);
        }
    }

    public void stop() {
        try {
            running = false;
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void scheduleQueueReader(final ExecutorService es, final BlockingQueue<E> queue, final ItemProcessor<E> processor) {
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
            final BlockingQueue<E> queue,
            final ItemProcessor<E> processor) {
        return () -> {
            final List<E> batch = new ArrayList<>(MAX_BATCH_SIZE);
            ProcessingDirective directive;

            while (!queue.isEmpty() || running) {
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
                        Iterator<E> itr = batch.iterator();
                        do {
                            directive = processor.process(itr.next());
                            itr.remove();
                        } while (itr.hasNext() && directive == CONTINUE);
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
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static <E> E pollQuietly(final BlockingQueue<E> queue, final long timeout, final TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static <E> boolean offerQuietly(final BlockingQueue<E> queue, final E item, final long timeout, final TimeUnit unit) {
        try {
            return queue.offer(item, timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static ThreadFactory createThreadFactory(final ThreadGroup group, final String nameFormat) {
        return new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setThreadFactory((runnable) -> new Thread(group, runnable))
            .build();
    }
}
