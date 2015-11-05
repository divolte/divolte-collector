package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=BrowserSourceConfiguration.class, name = "browser"),
})
@ParametersAreNonnullByDefault
public abstract class SourceConfiguration {
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }
}
