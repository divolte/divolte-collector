package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import io.divolte.server.gcs.GoogleCloudStorageFlushingPool;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class GoogleCloudStorageSinkConfiguration extends SinkConfiguration {

    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    @ParametersAreNullableByDefault
    GoogleCloudStorageSinkConfiguration(final FileStrategyConfiguration fileStrategy) {
        this.fileStrategy = Optional.ofNullable(fileStrategy).orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("fileStrategy", fileStrategy);
    }

    @Override
    public SinkFactory getFactory() {
        return GoogleCloudStorageFlushingPool::new;
    }
}
