package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;

@ParametersAreNonnullByDefault
public final class SessionBinningFileStrategyConfiguration extends FileStrategyConfiguration {
    @JsonCreator
    private SessionBinningFileStrategyConfiguration(
            final int syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir,
            /*
             * Nasty hack here! We need to have a roll_every property on this object
             * in order to support the default configuration without breaking when
             * overriding to the session binning strategy vs. the file binning one.
             *
             * This will be fixed when we either drop support for session binning
             * or we'll move to a new config setup with separation in sources, mappings
             * and sinks, where there is no default setup anymore.
             *
             * This makes it valid configuration to declare roll_every on a configuration
             * for session binning flushing, although it has no effect.
             */
            @SuppressWarnings("unused")
            final Duration rollEvery) {
        super(Types.SESSION_BINNING, syncFileAfterRecords, syncFileAfterDuration, workingDir, publishDir);
    }

    @Override
    public String toString() {
        return "SessionBinningFileStrategyConfiguration [type=" + type + ", syncFileAfterRecords=" + syncFileAfterRecords + ", syncFileAfterDuration=" + syncFileAfterDuration + ", workingDir=" + workingDir + ", publishDir=" + publishDir + "]";
    }
}
