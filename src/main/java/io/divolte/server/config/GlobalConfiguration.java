package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class GlobalConfiguration {
    @Valid public final ServerConfiguration server;
    @Valid public final MapperConfiguration mapper;
    @Valid public final HdfsConfiguration hdfs;
    @Valid public final KafkaConfiguration kafka;

    @JsonCreator
    GlobalConfiguration(final ServerConfiguration server,
                        final MapperConfiguration mapper,
                        final HdfsConfiguration hdfs,
                        final KafkaConfiguration kafka) {
        this.server = Objects.requireNonNull(server);
        this.mapper = Objects.requireNonNull(mapper);
        this.hdfs = Objects.requireNonNull(hdfs);
        this.kafka = Objects.requireNonNull(kafka);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("server", server)
                .add("mapper", mapper)
                .add("hdfs", hdfs)
                .add("kafka", kafka)
                .toString();
    }
}
