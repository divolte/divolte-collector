package io.divolte.server.config;

import com.google.common.base.MoreObjects;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public abstract class SinkTypeConfiguration {

    public final boolean enabled;
    public final int bufferSize;
    public final int threads;

    protected SinkTypeConfiguration(final int bufferSize, final int threads, final boolean enabled) {
        this.bufferSize = bufferSize;
        this.threads = threads;
        this.enabled = enabled;
    }

    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this)
                .add("enabled", enabled)
                .add("bufferSize", bufferSize)
                .add("threads", threads);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }
}
