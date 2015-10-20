package io.divolte.server.config;

import java.time.Duration;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class SimpleRollingFileStrategyConfiguration extends FileStrategyConfiguration {
    public final Duration rollEvery;

    @JsonCreator
    public SimpleRollingFileStrategyConfiguration(
            final Duration rollEvery,
            final Integer syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir) {
        super(Types.SIMPLE_ROLLING_FILE, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
        this.rollEvery = rollEvery;
    }

    @Override
    public String toString() {
        return "SimpleRollingFileStrategyConfiguration [rollEvery=" + rollEvery + ", type=" + type + ", syncFileAfterRecords=" + syncFileAfterRecords + ", syncFileAfterDuration=" + syncFileAfterDuration + ", workingDir=" + workingDir + ", publishDir=" + publishDir + "]";
    }
}
