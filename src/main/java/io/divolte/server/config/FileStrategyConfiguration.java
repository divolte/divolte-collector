package io.divolte.server.config;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

@ParametersAreNonnullByDefault
public class FileStrategyConfiguration {
    private static final String DEFAULT_SYNC_FILE_AFTER_RECORDS = "1000";
    private static final String DEFAULT_SYNC_FILE_AFTER_DURATION = "30 seconds";
    private static final String DEFAULT_WORKING_DIR = "/tmp";
    private static final String DEFAULT_PUBLISH_DIR = "/tmp";
    private static final String DEFAULT_ROLL_EVERY = "1 hour";

    static final FileStrategyConfiguration DEFAULT_FILE_STRATEGY_CONFIGURATION =
            new FileStrategyConfiguration(
                    DurationDeserializer.parseDuration(DEFAULT_ROLL_EVERY),
                    Integer.parseInt(DEFAULT_SYNC_FILE_AFTER_RECORDS),
                    DurationDeserializer.parseDuration(DEFAULT_SYNC_FILE_AFTER_DURATION),
                    DEFAULT_WORKING_DIR,
                    DEFAULT_PUBLISH_DIR);

    public final int syncFileAfterRecords;
    public final Duration syncFileAfterDuration;
    public final String workingDir;
    public final String publishDir;
    public final Duration rollEvery;

    @JsonCreator
    @ParametersAreNullableByDefault
    FileStrategyConfiguration(@JsonProperty(defaultValue=DEFAULT_ROLL_EVERY) final Duration rollEvery,
                              @JsonProperty(defaultValue=DEFAULT_SYNC_FILE_AFTER_RECORDS) final Integer syncFileAfterRecords,
                              @JsonProperty(defaultValue=DEFAULT_SYNC_FILE_AFTER_DURATION) final Duration syncFileAfterDuration,
                              @JsonProperty(defaultValue=DEFAULT_WORKING_DIR) final String workingDir,
                              @JsonProperty(defaultValue=DEFAULT_PUBLISH_DIR) final String publishDir) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.rollEvery = Optional.ofNullable(rollEvery).orElseGet(() -> DurationDeserializer.parseDuration(DEFAULT_ROLL_EVERY));
        this.syncFileAfterRecords = Optional.ofNullable(syncFileAfterRecords).orElseGet(() -> Integer.valueOf(DEFAULT_SYNC_FILE_AFTER_RECORDS));
        this.syncFileAfterDuration = Optional.ofNullable(syncFileAfterDuration).orElseGet(() -> DurationDeserializer.parseDuration(DEFAULT_SYNC_FILE_AFTER_DURATION));
        this.workingDir = Optional.ofNullable(workingDir).orElse(DEFAULT_WORKING_DIR);
        this.publishDir = Optional.ofNullable(publishDir).orElse(DEFAULT_PUBLISH_DIR);
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
