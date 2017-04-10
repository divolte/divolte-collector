package io.divolte.server.config;

import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.divolte.server.filesinks.FileFlushingPool;
import io.divolte.server.filesinks.FileManager.FileManagerFactory;
import io.divolte.server.filesinks.hdfs.HdfsFileManager;

@ParametersAreNonnullByDefault
public class HdfsSinkConfiguration extends FileSinkConfiguration {
    private static final String DEFAULT_REPLICATION = "3";

    public final short replication;

    @JsonCreator
    @ParametersAreNullableByDefault
    HdfsSinkConfiguration(@JsonProperty(defaultValue=DEFAULT_REPLICATION) final Short replication,
                          final FileStrategyConfiguration fileStrategy) {
        super(fileStrategy);
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.replication = Optional.ofNullable(replication).orElseGet(() -> Short.valueOf(DEFAULT_REPLICATION));
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("replication", replication)
                .add("fileStrategy", fileStrategy);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final FileManagerFactory fileManagerFactory = HdfsFileManager.newFactory(config, name, registry.getSchemaBySinkName(name));
            fileManagerFactory.verifyFileSystemConfiguration();
            return new FileFlushingPool(config, name, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Hadoop FileSystem (HDFS)";
    }
}
