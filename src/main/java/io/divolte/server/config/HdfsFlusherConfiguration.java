package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class HdfsFlusherConfiguration {
    public final boolean enabled;
    public final int threads;
    public final int maxWriteQueue;
    public final Duration maxEnqueueDelay;
    public final HdfsConfiguration hdfs;
    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    private HdfsFlusherConfiguration(
            final boolean enabled,
            final int threads,
            final int maxWriteQueue,
            final Duration maxEnqueueDelay,
            final HdfsConfiguration hdfs,
            final FileStrategyConfiguration fileStrategy) {
        this.enabled = enabled;
        this.threads = threads;
        this.maxWriteQueue = maxWriteQueue;
        this.maxEnqueueDelay = Objects.requireNonNull(maxEnqueueDelay);
        this.hdfs = Objects.requireNonNull(hdfs);
        this.fileStrategy = Objects.requireNonNull(fileStrategy);
    }

    @Override
    public String toString() {
        return "HdfsFlusherConfiguration [enabled=" + enabled + ", threads=" + threads + ", maxWriteQueue=" + maxWriteQueue + ", maxEnqueueDelay=" + maxEnqueueDelay + ", hdfs=" + hdfs + ", fileStrategy=" + fileStrategy + "]";
    }
}
