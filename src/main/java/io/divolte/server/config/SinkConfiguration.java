package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.SchemaRegistry;
import io.divolte.server.processing.ProcessingPool;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=HdfsSinkConfiguration.class, name = "hdfs"),
        @JsonSubTypes.Type(value=KafkaSinkConfiguration.class, name = "kafka"),
})
@ParametersAreNonnullByDefault
public abstract class SinkConfiguration {
    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public final String toString() {
        return toStringHelper().toString();
    }

    @JsonIgnore
    public abstract SinkFactory getFactory();

    @FunctionalInterface
    public interface SinkFactory {
        ProcessingPool<?, AvroRecordBuffer> create(ValidatedConfiguration configuration,
                                                   String sinkName,
                                                   SchemaRegistry schemaRegistry);
    }
}
