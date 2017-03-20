package io.divolte.server.config;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import io.divolte.server.filesinks.FileFlushingPool;
import io.divolte.server.filesinks.FileManager.FileManagerFactory;
import io.divolte.server.filesinks.gcs.GoogleCloudStorageFileManager;

@ParametersAreNonnullByDefault
public class GoogleCloudStorageSinkConfiguration extends FileSinkConfiguration {
    @JsonCreator
    @ParametersAreNullableByDefault
    GoogleCloudStorageSinkConfiguration(final FileStrategyConfiguration fileStrategy) {
        super(fileStrategy);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("fileStrategy", fileStrategy);
    }

    @Override
    public SinkFactory getFactory() {
        return (config, name, registry) -> {
            final FileManagerFactory fileManagerFactory = GoogleCloudStorageFileManager.newFactory(config, name, registry.getSchemaBySinkName(name));
            fileManagerFactory.verifyFileSystemConfiguration();
            final Schema schema = registry.getSchemaBySinkName(name);
            return new FileFlushingPool(config, name, schema, fileManagerFactory);
        };
    }

    @Override
    public String getReadableType() {
        return "Google Cloud Storage";
    }
}
