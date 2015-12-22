package io.divolte.server.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.divolte.server.config.constraint.MappingSourceSinkReferencesMustExist;
import io.divolte.server.config.constraint.OneSchemaPerSink;
import io.divolte.server.config.constraint.SourceAndSinkNamesCannotCollide;

@ParametersAreNonnullByDefault
@MappingSourceSinkReferencesMustExist
@SourceAndSinkNamesCannotCollide
@OneSchemaPerSink
public final class DivolteConfiguration {
    @Valid public final GlobalConfiguration global;
    @Valid public final ImmutableMap<String,SourceConfiguration> sources;
    @Valid public final ImmutableMap<String,SinkConfiguration> sinks;
    @Valid public final ImmutableMap<String,MappingConfiguration> mappings;

    @Deprecated
    public final MappingConfiguration incomingRequestProcessor;
    @Deprecated
    public final BrowserSourceConfiguration browserSourceConfiguration;
    @Deprecated
    public final KafkaSinkConfiguration kafkaFlusher;
    @Deprecated
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
        this.incomingRequestProcessor = Iterables.get(this.mappings.values(), 0);
        this.browserSourceConfiguration = (BrowserSourceConfiguration) Iterables.get(this.sources.values(), 0);
        this.kafkaFlusher = (KafkaSinkConfiguration) Iterators.get(this.sinks.values().stream().filter((sink) -> sink instanceof KafkaSinkConfiguration).iterator(), 0);
        this.hdfsFlusher = (HdfsSinkConfiguration) Iterators.get(this.sinks.values().stream().filter((sink) -> sink instanceof HdfsSinkConfiguration).iterator(), 0);

        // TODO: Optimizations:
        //  - Elide HDFS and Kafka sinks if they are globally disabled.
        //  - Elide unreferenced sources and sinks.
    }

    // Defaults; these will eventually disappear
    private static ImmutableMap<String,SourceConfiguration> defaultSourceConfigurations() {
        return ImmutableMap.of("browser", BrowserSourceConfiguration.DEFAULT_BROWSER_SOURCE_CONFIGURATION);
    }

    private static ImmutableMap<String,SinkConfiguration> defaultSinkConfigurations() {
        return ImmutableMap.of("hdfs", new HdfsSinkConfiguration((short) 1, FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION),
                               "kafka", new KafkaSinkConfiguration(null));
    }

    private static ImmutableMap<String,MappingConfiguration> defaultMappingConfigurations(final ImmutableSet<String> sourceNames,
                                                                                          final ImmutableSet<String> sinkNames) {
        return ImmutableMap.of("default", new MappingConfiguration(Optional.empty(),
                                                                   Optional.empty(),
                                                                   sourceNames,
                                                                   sinkNames,
                                                                   false,
                                                                   false));
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

    /*
     * Validation support methods here.
     *
     * As bean validation uses expression language for rendering error messages,
     * substitutions need to be available for some of these. EL doesn't allow for
     * access to attributes, just getters/setters and methods. Hence, here are a
     * number of methods that are used to render validation messages. These result
     * of these methods can also be used for actual validation.
     */
    public Set<String> missingSourcesSinks() {
        final Set<String> defined = new HashSet<>();
        defined.addAll(sources.keySet());
        defined.addAll(sinks.keySet());

        final Set<String> used = mappings
                .values()
                .stream()
                .flatMap(mc -> Stream.concat(
                        mc.sources.stream(),
                        mc.sinks.stream()))
                .collect(Collectors.toSet());

        return Sets.difference(used, defined);
    }

    public Set<String> collidingSourceAndSinkNames() {
        return Sets.intersection(sources.keySet(), sinks.keySet());
    }

    public Set<String> sinksWithMultipleSchemas() {
        final Map<String,List<String>> sinkSchemas = new HashMap<>();
        for (final MappingConfiguration mc : mappings.values()) {
            for (final String s : mc.sinks) {
                sinkSchemas.computeIfAbsent(s, i -> new ArrayList<>()).add(mc.schemaFile.orElse("<default>"));
            }
        }

        return sinkSchemas.entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}
