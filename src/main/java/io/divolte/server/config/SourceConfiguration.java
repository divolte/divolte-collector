package io.divolte.server.config;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;

import io.divolte.server.HttpSource;
import io.divolte.server.IncomingRequestProcessingPool;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=BrowserSourceConfiguration.class, name = "browser"),
        @JsonSubTypes.Type(value=JsonSourceConfiguration.class, name = "json"),
})
@ParametersAreNonnullByDefault
public abstract class SourceConfiguration {
    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }

    public abstract HttpSource createSource(
            ValidatedConfiguration configuration,
            String sourceName,
            IncomingRequestProcessingPool processingPool);
}
