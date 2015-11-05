package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class DivolteConfiguration {
    @Valid public final GlobalConfiguration global;
    @Valid public final ImmutableMap<String,SourceConfiguration> sources;
    @Valid public final ImmutableMap<String,SinkConfiguration> sinks;
    @Valid public final ImmutableMap<String,MappingConfiguration> mappings;

    /** @deprecated */
    public final MappingConfiguration incomingRequestProcessor;
    /** @deprecated */
    public final BrowserSourceConfiguration browserSourceConfiguration;
    /** @deprecated */
    public final KafkaSinkConfiguration kafkaFlusher;
    /** @deprecated */
    public final HdfsSinkConfiguration hdfsFlusher;

    @JsonCreator
    DivolteConfiguration(final GlobalConfiguration global,
                         final Optional<ImmutableMap<String, SourceConfiguration>> sources,
                         final Optional<ImmutableMap<String, SinkConfiguration>> sinks,
                         final Optional<ImmutableMap<String,MappingConfiguration>> mappings) {
        this.sources = sources.orElseGet(DivolteConfiguration::defaultSourceConfigurations);
        this.sinks = sinks.orElseGet(DivolteConfiguration::defaultSinkConfigurations);
        this.mappings = mappings.orElseGet(() -> defaultMappingConfigurations(this.sources.keySet(), this.sinks.keySet()));
        this.global = Objects.requireNonNull(global);
        // Temporary interop
        this.incomingRequestProcessor = Iterables.getOnlyElement(this.mappings.values());
        this.browserSourceConfiguration = (BrowserSourceConfiguration) Iterables.getOnlyElement(this.sources.values());
        this.kafkaFlusher = (KafkaSinkConfiguration) Iterators.getOnlyElement(this.sinks.values().stream().filter((sink) -> sink instanceof KafkaSinkConfiguration).iterator());
        this.hdfsFlusher = (HdfsSinkConfiguration) Iterators.getOnlyElement(this.sinks.values().stream().filter((sink) -> sink instanceof HdfsSinkConfiguration).iterator());
        // TODO: Validate that the mappings refer to defined sources and sinks.
        // TODO: Validate that all mappings that refer to a sink have the same schema.

        // TODO: Optimizations:
        //  - Elide HDFS and Kafka sinks if they are globally disabled.
        //  - Elide unreferenced sources and sinks.
    }

    private static ImmutableMap<String,SourceConfiguration> defaultSourceConfigurations() {
        return ImmutableMap.of("browser", new BrowserSourceConfiguration(Optional.empty(),
                                                                         Optional.empty(),
                                                                         Optional.empty(),
                                                                         Optional.empty(),
                                                                         Optional.empty(),
                                                                         Optional.empty(),
                                                                         Optional.empty()));
    }

    private static ImmutableMap<String,SinkConfiguration> defaultSinkConfigurations() {
        return ImmutableMap.of("hdfs", new HdfsSinkConfiguration(Optional.empty(), Optional.empty()),
                               "kafka", new KafkaSinkConfiguration(Optional.empty()));
    }

    private static ImmutableMap<String,MappingConfiguration> defaultMappingConfigurations(final ImmutableSet<String> sourceNames,
                                                                                          final ImmutableSet<String> sinkNames) {
        return ImmutableMap.of("default", new MappingConfiguration(Optional.empty(),
                                                                   Optional.empty(),
                                                                   sourceNames,
                                                                   sinkNames,
                                                                   Optional.empty(),
                                                                   Optional.empty()));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("global", global)
                .add("sources", sources)
                .add("sinks", sinks)
                .add("mappings", mappings)
                .toString();
    }
}
