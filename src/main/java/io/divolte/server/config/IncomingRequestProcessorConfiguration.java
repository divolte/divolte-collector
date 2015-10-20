package io.divolte.server.config;

import java.time.Duration;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class IncomingRequestProcessorConfiguration {
    public final Integer threads;
    public final Integer maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final Boolean discardCorrupted;
    public final Integer duplicateMemorySize;
    public final Boolean discardDuplicates;

    @JsonCreator
    private IncomingRequestProcessorConfiguration(
            final Integer threads,
            final Integer maxWriteQueue,
            final Duration maxEnqueueDelay,
            final Boolean discardCorrupted,
            final Integer duplicateMemorySize,
            final Boolean discardDuplicates) {
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
