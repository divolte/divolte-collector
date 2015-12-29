package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import io.divolte.server.config.constraint.MappingSourceSinkReferencesMustExist;
import io.divolte.server.config.constraint.OneSchemaPerSink;
import io.divolte.server.config.constraint.SourceAndSinkNamesCannotCollide;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ParametersAreNonnullByDefault
@MappingSourceSinkReferencesMustExist
@SourceAndSinkNamesCannotCollide
@OneSchemaPerSink
public final class DivolteConfiguration {
    @Valid public final GlobalConfiguration global;

    // Mappings, sources and sinks are all keyed by their name.
    @Valid public final ImmutableMap<String,MappingConfiguration> mappings;
    @Valid public final ImmutableMap<String,SourceConfiguration> sources;
    @Valid public final ImmutableMap<String,SinkConfiguration> sinks;

    @JsonCreator
    DivolteConfiguration(final GlobalConfiguration global,
                         final Optional<ImmutableMap<String, SourceConfiguration>> sources,
                         final Optional<ImmutableMap<String, SinkConfiguration>> sinks,
                         final Optional<ImmutableMap<String,MappingConfiguration>> mappings) {
        this.global = Objects.requireNonNull(global);
        this.sources = sources.orElseGet(DivolteConfiguration::defaultSourceConfigurations);
        this.sinks = sinks.orElseGet(DivolteConfiguration::defaultSinkConfigurations);
        this.mappings = mappings.orElseGet(() -> defaultMappingConfigurations(this.sources.keySet(), this.sinks.keySet()));
    }

    public BrowserSourceConfiguration getBrowserSourceConfiguration(final String sourceName) {
        final SourceConfiguration sourceConfiguration = sources.get(sourceName);
        Objects.requireNonNull(sourceConfiguration, () -> "No source configuration with name: " + sourceName);
        Preconditions.checkArgument(sourceConfiguration instanceof BrowserSourceConfiguration,
                                    "Source configuration '%s' is not a browser source", sourceName);
        return (BrowserSourceConfiguration)sourceConfiguration;
    }

    public MappingConfiguration getMappingConfiguration(final String mappingName) {
        final MappingConfiguration mappingConfiguration = mappings.get(mappingName);
        Objects.requireNonNull(mappingConfiguration, () -> "No mapping configuration with name: " + mappingName);
        return mappingConfiguration;
    }

    public <T> T getSinkConfiguration(final String sinkName, final Class <? extends T> sinkClass) {
        final SinkConfiguration sinkConfiguration = sinks.get(sinkName);
        Objects.requireNonNull(sinkConfiguration, () -> "No sink configuration with name: " + sinkName);
        Preconditions.checkArgument(sinkClass.isInstance(sinkConfiguration),
                                    "Sink configuration '%s' is not a %s sink", sinkName, sinkClass.getSimpleName());
        return sinkClass.cast(sinkConfiguration);
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
        final Map<String, Long> countsBySink =
                mappings.values()
                        .stream()
                        .flatMap(config -> config.sinks.stream()
                                                       .map(sink -> Maps.immutableEntry(sink, config.schemaFile)))
                        .distinct()
                        .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.counting()));
        return Maps.filterValues(countsBySink, count -> count > 1L).keySet();
    }
}
