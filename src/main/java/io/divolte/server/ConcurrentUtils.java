package io.divolte.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

final class ConcurrentUtils {
    public static <E> E pollQuietly(final LinkedBlockingQueue<E> queue, long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public static <T> boolean offerQuietly(final LinkedBlockingQueue<T> queue, T item, long timeout, TimeUnit unit) {
        try {
            return queue.offer(item, timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static ThreadFactory createThreadFactory(ThreadGroup group, String nameFormat) {
        return new ThreadFactoryBuilder()
        .setNameFormat(nameFormat)
        .setThreadFactory((runnable) -> new Thread(group, runnable))
        .build();
    }

    public static <T> Runnable microBatchingQueueDrainer(final LinkedBlockingQueue<T> queue, final Consumer<T> consumer) {
        return () -> {
            final int maxBatchSize = 100;
            final List<T> batch = new ArrayList<>(maxBatchSize);

            while(!Thread.currentThread().isInterrupted() || !queue.isEmpty()) {
                queue.drainTo(batch, maxBatchSize - 1);
                final int batchSize = batch.size();

                batch.forEach(consumer);
                batch.clear();

                // if the batch was empty, block on the queue for some time until something is available
                final T polled;
                if (batchSize == 0 && (polled = pollQuietly(queue, 1, TimeUnit.SECONDS)) != null) {
                    batch.add(polled);
                }
            }
        };
    }
}
