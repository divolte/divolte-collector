package io.divolte.server.config;

public class GoogleCloudStorageConfiguration extends SinkTypeConfiguration {
    protected GoogleCloudStorageConfiguration(final int bufferSize, final int threads, final boolean enabled) {
        super(bufferSize, threads, enabled);
    }
}
