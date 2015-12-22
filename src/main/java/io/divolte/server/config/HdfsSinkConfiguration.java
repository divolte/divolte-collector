package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class HdfsSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_REPLICATION = "3";

    public final short replication;
    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    HdfsSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_REPLICATION) final Short replication,
                          final FileStrategyConfiguration fileStrategy) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue proprty from the annotation to fix this
        this.replication = replication == null ? Short.valueOf(DEFAULT_REPLICATION) : replication;
        /*
         * Passing a null defaults to the default strategy. Reason for not making the parameter Optional<...> is
         * that this way, we can at some point use a tool to automatically document the configuration objects
         * including types. This type of defaults could then be documented through the parameter specific JavaDoc
         * for that param.
         */
        this.fileStrategy = Optional.ofNullable(fileStrategy).orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("replication", replication)
                .add("fileStrategy", fileStrategy);
    }
}
