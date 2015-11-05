package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class HdfsSinkConfiguration extends SinkConfiguration {
    private static final short DEFAULT_REPLICATION = 3;

    public final short replication;
    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    HdfsSinkConfiguration(final Optional<Short> replication,
                          final Optional<FileStrategyConfiguration> fileStrategy) {
        this.replication = replication.orElse(DEFAULT_REPLICATION);
        this.fileStrategy = fileStrategy.orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("replication", replication)
                .add("fileStrategy", fileStrategy);
    }
}
