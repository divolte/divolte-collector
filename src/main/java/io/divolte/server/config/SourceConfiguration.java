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
        @JsonSubTypes.Type(value=MobileSourceConfiguration.class, name = "mobile"),
})
@ParametersAreNonnullByDefault
public abstract class SourceConfiguration {
    protected static final String DEFAULT_PREFIX = "/";

    public final String prefix;

    protected SourceConfiguration(final String prefix) {
        this.prefix = ensureTrailingSlash(prefix == null ? DEFAULT_PREFIX : prefix);
    }

    private static String ensureTrailingSlash(final String s) {
        return s.endsWith("/") ? s : s + '/';
    }

    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this)
                .add("prefix", prefix);
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
