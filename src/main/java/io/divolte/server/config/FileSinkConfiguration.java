package io.divolte.server.config;

import java.util.Optional;

public abstract class FileSinkConfiguration extends SinkConfiguration {
    public final FileStrategyConfiguration fileStrategy;

    public FileSinkConfiguration(final FileStrategyConfiguration fileStrategy) {
        super();
        this.fileStrategy = Optional.ofNullable(fileStrategy).orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    public abstract String getReadableType();
}
