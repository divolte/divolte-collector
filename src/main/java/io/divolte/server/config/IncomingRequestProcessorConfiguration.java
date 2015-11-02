package io.divolte.server.config;

import java.time.Duration;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class IncomingRequestProcessorConfiguration {
    public final int threads;
    public final int maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final boolean discardCorrupted;
    public final int duplicateMemorySize;
    public final boolean discardDuplicates;

    @JsonCreator
    private IncomingRequestProcessorConfiguration(
            final int threads,
            final int maxWriteQueue,
            final Duration maxEnqueueDelay,
            final boolean discardCorrupted,
            final int duplicateMemorySize,
            final boolean discardDuplicates) {
        this.threads = threads;
        this.maxWriteQueue = maxWriteQueue;
        this.maxEnqueueDelay = maxEnqueueDelay;
        this.discardCorrupted = discardCorrupted;
        this.duplicateMemorySize = duplicateMemorySize;
        this.discardDuplicates = discardDuplicates;
    }

    @Override
    public String toString() {
        return "IncomingRequestProcessorConfiguration [threads=" + threads + ", maxWriteQueue=" + maxWriteQueue + ", maxEnqueueDelay=" + maxEnqueueDelay + ", discardCorrupted=" + discardCorrupted + ", duplicateMemorySize=" + duplicateMemorySize + ", discardDuplicates=" + discardDuplicates + "]";
    }
}
