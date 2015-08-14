package io.divolte.server.config;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value=SimpleRollingFileStrategyConfiguration.class, name = "SIMPLE_ROLLING_FILE"),
    @Type(value=SessionBinningFileStrategyConfiguration.class, name = "SESSION_BINNING")
})
public abstract class FileStrategyConfiguration {
    public final FileStrategyConfiguration.Types type;
    public final Integer syncFileAfterRecords;
    public final Duration syncFileAfterDuration;
    public final String workingDir;
    public final String publishDir;

    @JsonCreator
    public FileStrategyConfiguration (
            final FileStrategyConfiguration.Types type,
            final Integer syncFileAfterRecords,
            final Duration syncFileAfterDuration,
            final String workingDir,
            final String publishDir) {
        this.type = type;
        this.syncFileAfterRecords = syncFileAfterRecords;
        this.syncFileAfterDuration = syncFileAfterDuration;
        this.workingDir = workingDir;
        this.publishDir = publishDir;
    }

    public static enum Types {
        SIMPLE_ROLLING_FILE(SimpleRollingFileStrategyConfiguration.class),
        SESSION_BINNING(SessionBinningFileStrategyConfiguration.class);
        
        public final Class<?> clazz;
        Types(Class<?> clazz) { this.clazz = clazz; }
    }
    
    public <T> T as(Class<T> target) {
        Preconditions.checkState(type.clazz.equals(target),
                                 "Attempt to cast FileStrategyConfiguration to wrong type.");
        return target.cast(this);
    }
}