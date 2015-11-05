package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class FileStrategyConfiguration {
    private static final int DEFAULT_SYNC_FILE_AFTER_RECORDS = 1000;
    private static final Duration DEFAULT_SYNC_FILE_AFTER_DURATION = Duration.ofSeconds(30);
    private static final String DEFAULT_WORKING_DIR = "/tmp";
    private static final String DEFAULT_PUBLISH_DIR = "/tmp";
    private static final Duration DEFAULT_ROLL_EVERY = Duration.ofHours(1);

    static final FileStrategyConfiguration DEFAULT_FILE_STRATEGY_CONFIGURATION =
            new FileStrategyConfiguration(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    public final int syncFileAfterRecords;
    public final Duration syncFileAfterDuration;
    public final String workingDir;
    public final String publishDir;
    public final Duration rollEvery;

    @JsonCreator
    FileStrategyConfiguration(final Optional<Duration> rollEvery,
                              final Optional<Integer> syncFileAfterRecords,
                              final Optional<Duration> syncFileAfterDuration,
                              final Optional<String> workingDir,
                              final Optional<String> publishDir) {
        this.rollEvery = rollEvery.orElse(DEFAULT_ROLL_EVERY);
        this.syncFileAfterRecords = syncFileAfterRecords.orElse(DEFAULT_SYNC_FILE_AFTER_RECORDS);
        this.syncFileAfterDuration = syncFileAfterDuration.orElse(DEFAULT_SYNC_FILE_AFTER_DURATION);
        this.workingDir = workingDir.orElse(DEFAULT_WORKING_DIR);
        this.publishDir = publishDir.orElse(DEFAULT_PUBLISH_DIR);
    }

    @Override
    public final String toString() {
        return MoreObjects.toStringHelper(this)
                .add("rollEvery", rollEvery)
                .add("syncFileAfterRecords", syncFileAfterRecords)
                .add("syncFileAfterDuration", syncFileAfterDuration)
                .add("workingDir", workingDir)
                .add("publishDir", publishDir).toString();
    }
}
