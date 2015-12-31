package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableCollection;
import io.divolte.server.DivolteEvent;
import io.divolte.server.HttpSource;
import io.divolte.server.processing.ProcessingPool;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

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

    @JsonIgnore
    public abstract SourceFactory getFactory();

    @FunctionalInterface
    public interface SourceFactory {
        HttpSource create(ValidatedConfiguration configuration,
                          String sourceName,
                          ImmutableCollection<? extends ProcessingPool<?, DivolteEvent>> mappingProcessors);
    }
}
