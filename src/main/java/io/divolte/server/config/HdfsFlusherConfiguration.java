package io.divolte.server.config;

import java.time.Duration;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class HdfsFlusherConfiguration {
    public final Boolean enabled;
    public final Integer threads;
    public final Integer maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final HdfsConfiguration hdfs;
    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    private HdfsFlusherConfiguration(
            final Boolean enabled,
            final Integer threads,
            final Integer maxWriteQueue,
            final Duration maxEnqueueDelay,
            final HdfsConfiguration hdfs,
            final FileStrategyConfiguration fileStrategy) {
        this.enabled = enabled;
        this.threads = threads;
        this.maxWriteQueue = maxWriteQueue;
        this.maxEnqueueDelay = maxEnqueueDelay;
        this.hdfs = hdfs;
        this.fileStrategy = fileStrategy;
    }

    @Override
    public String toString() {
        return "HdfsFlusherConfiguration [enabled=" + enabled + ", threads=" + threads + ", maxWriteQueue=" + maxWriteQueue + ", maxEnqueueDelay=" + maxEnqueueDelay + ", hdfs=" + hdfs + ", fileStrategy=" + fileStrategy + "]";
    }
}
