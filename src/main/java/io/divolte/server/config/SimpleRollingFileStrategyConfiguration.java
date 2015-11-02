package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class SimpleRollingFileStrategyConfiguration extends FileStrategyConfiguration {
    public final Duration rollEvery;

    @JsonCreator
    private SimpleRollingFileStrategyConfiguration(
            final Duration rollEvery,
            final int syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir) {
        super(Types.SIMPLE_ROLLING_FILE, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
        this.rollEvery = Objects.requireNonNull(rollEvery);
    }

    @Override
    public String toString() {
        return "SimpleRollingFileStrategyConfiguration [rollEvery=" + rollEvery + ", type=" + type + ", syncFileAfterRecords=" + syncFileAfterRecords + ", syncFileAfterDuration=" + syncFileAfterDuration + ", workingDir=" + workingDir + ", publishDir=" + publishDir + "]";
    }
}
