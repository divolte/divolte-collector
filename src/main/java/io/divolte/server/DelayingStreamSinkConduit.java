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

import io.undertow.util.WorkerUtils;
import org.xnio.channels.StreamSourceChannel;
import org.xnio.conduits.AbstractStreamSinkConduit;
import org.xnio.conduits.StreamSinkConduit;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

class DelayingStreamSinkConduit extends AbstractStreamSinkConduit<StreamSinkConduit> {
    private static final int NANOS_PER_SECOND = (int) TimeUnit.SECONDS.toNanos(1);

    private final long blockUntil;

    public DelayingStreamSinkConduit(final StreamSinkConduit next, final long delay, final TimeUnit timeUnit) {
        super(next);
        blockUntil = System.nanoTime() + timeUnit.toNanos(delay);
    }

    private boolean writesResumed;
    private boolean scheduled;

    @Override
    public int write(final ByteBuffer src) throws IOException {
        return canSend() ? super.write(src) : 0;
    }

    @Override
    public long transferFrom(final FileChannel src, final long position, final long count) throws IOException {
        return canSend() ? super.transferFrom(src, position, count) : 0;
    }

    @Override
    public long transferFrom(final StreamSourceChannel source, final long count, final ByteBuffer throughBuffer) throws IOException {
        return canSend() ? super.transferFrom(source, count, throughBuffer) : 0;
    }

    @Override
    public long write(final ByteBuffer[] srcs, final int offs, final int len) throws IOException {
        return canSend() ? super.write(srcs, offs, len) : 0;
    }

    @Override
    public int writeFinal(final ByteBuffer src) throws IOException {
        return canSend() ? super.writeFinal(src) : 0;
    }

    @Override
    public long writeFinal(final ByteBuffer[] srcs, final int offs, final int len) throws IOException {
        return canSend() ? super.writeFinal(srcs, offs, len) : 0;
    }

    @Override
    public void resumeWrites() {
        writesResumed = true;
        if (canSend()) {
            super.resumeWrites();
        }
    }

    @Override
    public void suspendWrites() {
        writesResumed = false;
        super.suspendWrites();
    }

    @Override
    public void wakeupWrites() {
        writesResumed = true;
        if (canSend()) {
            super.wakeupWrites();
        }
    }

    @Override
    public boolean isWriteResumed() {
        return writesResumed;
    }

    @Override
    public void awaitWritable() throws IOException {
        final long remaining = blockUntil - System.nanoTime();
        if (0 < remaining) {
            try {
                Thread.sleep(remaining / NANOS_PER_SECOND, (int)(remaining % NANOS_PER_SECOND));
            } catch (final InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
        super.awaitWritable();
    }

    @Override
    public void awaitWritable(final long time, final TimeUnit timeUnit) throws IOException {
        final long startTime = System.nanoTime();
        final long remaining = blockUntil - startTime;
        if (0 < remaining) {
            try {
                final long sleepNanos = Math.min(remaining, timeUnit.toNanos(time));
                Thread.sleep(sleepNanos / NANOS_PER_SECOND, (int)(sleepNanos % NANOS_PER_SECOND));
            } catch (final InterruptedException e) {
                throw new InterruptedIOException();
            }
            final long elapsed = System.nanoTime() - startTime;
            final long stillRemaining = timeUnit.toNanos(time) - elapsed;
            if (0 < stillRemaining) {
                super.awaitWritable(stillRemaining, TimeUnit.NANOSECONDS);
            }
        } else {
            super.awaitWritable(time, timeUnit);
        }
    }

    private boolean canSend() {
        final long remaining = blockUntil - System.nanoTime();
        final boolean canSend = 0 > remaining;
        if (!canSend && writesResumed) {
            handleWritesResumedWhenBlocked();
        }
        return canSend;
    }

    private void handleWritesResumedWhenBlocked() {
        if (!scheduled) {
            scheduled = true;
            next.suspendWrites();
            final long remaining = blockUntil - System.nanoTime();
            WorkerUtils.executeAfter(getWriteThread(), () -> {
                scheduled = false;
                if (writesResumed) {
                    next.wakeupWrites();
                }
            }, remaining, TimeUnit.NANOSECONDS);
        }
    }
}
