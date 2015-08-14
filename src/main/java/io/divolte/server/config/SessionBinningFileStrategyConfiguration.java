package io.divolte.server.config;

import java.time.Duration;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class SessionBinningFileStrategyConfiguration extends FileStrategyConfiguration {
    @JsonCreator
    public SessionBinningFileStrategyConfiguration(
            final Integer syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir) {
        super(Types.SESSION_BINNING, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
    }

    @Override
    public String toString() {
        return "SessionBinningFileStrategyConfiguration [type=" + type + ", syncFileAfterRecords=" + syncFileAfterRecords + ", syncFileAfterDuration=" + syncFileAfterDuration + ", workingDir=" + workingDir + ", publishDir=" + publishDir + "]";
    }
}