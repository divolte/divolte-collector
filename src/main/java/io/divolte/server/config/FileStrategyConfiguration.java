package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value=SimpleRollingFileStrategyConfiguration.class, name = "SIMPLE_ROLLING_FILE"),
    @Type(value=SessionBinningFileStrategyConfiguration.class, name = "SESSION_BINNING")
})
@ParametersAreNonnullByDefault
public abstract class FileStrategyConfiguration {
    public final FileStrategyConfiguration.Types type;
    public final int syncFileAfterRecords;
    public final Duration syncFileAfterDuration;
    public final String workingDir;
    public final String publishDir;

    protected FileStrategyConfiguration (
            final FileStrategyConfiguration.Types type,
            final int syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir) {
        this.type = Objects.requireNonNull(type);
        this.syncFileAfterRecords = Objects.requireNonNull(syncFileAfterRecords);
        this.syncFileAfterDuration = Objects.requireNonNull(syncFileAfterDuration);
        this.workingDir = Objects.requireNonNull(workingDir);
        this.publishDir = Objects.requireNonNull(publishDir);
    }

    @ParametersAreNonnullByDefault
    public enum Types {
        SIMPLE_ROLLING_FILE(SimpleRollingFileStrategyConfiguration.class),
        SESSION_BINNING(SessionBinningFileStrategyConfiguration.class);

        public final Class<?> clazz;

        Types(final Class<?> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }
    }

    public <T> T as(Class<T> target) {
        Preconditions.checkState(type.clazz.equals(target),
                                 "Attempt to cast FileStrategyConfiguration to wrong type.");
        return target.cast(this);
    }
}
