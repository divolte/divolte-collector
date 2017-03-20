package io.divolte.server.config;

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

@ParametersAreNonnullByDefault
public class GlobalConfiguration {
    @Valid public final ServerConfiguration server;
    @Valid public final MapperConfiguration mapper;
    @Valid public final HdfsConfiguration hdfs;
    @Valid public final KafkaConfiguration kafka;
    @Valid public final GoogleCloudStorageConfiguration gcs;

    @JsonCreator
    GlobalConfiguration(final ServerConfiguration server,
                        final MapperConfiguration mapper,
                        final HdfsConfiguration hdfs,
                        final KafkaConfiguration kafka,
                        final GoogleCloudStorageConfiguration gcs) {
        this.server = Objects.requireNonNull(server);
        this.mapper = Objects.requireNonNull(mapper);
        this.hdfs = Objects.requireNonNull(hdfs);
        this.kafka = Objects.requireNonNull(kafka);
        this.gcs = gcs;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("server", server)
                .add("mapper", mapper)
                .add("hdfs", hdfs)
                .add("gcs", gcs)
                .add("kafka", kafka)
                .toString();
    }
}
