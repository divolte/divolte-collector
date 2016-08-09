package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.divolte.server.hdfs.HdfsFlushingPool;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class HdfsSinkConfiguration extends SinkConfiguration {
    private static final String DEFAULT_REPLICATION = "3";

    public final short replication;
    public final FileStrategyConfiguration fileStrategy;

    @JsonCreator
    @ParametersAreNullableByDefault
    HdfsSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_REPLICATION) final Short replication,
                          final FileStrategyConfiguration fileStrategy) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.replication = Optional.ofNullable(replication).orElseGet(() -> Short.valueOf(DEFAULT_REPLICATION));
        this.fileStrategy = Optional.ofNullable(fileStrategy).orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("replication", replication)
                .add("fileStrategy", fileStrategy);
    }

    @Override
    public SinkFactory getFactory() {
        return HdfsFlushingPool::new;
    }
}
