package io.divolte.server.config;

import java.util.HashSet;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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

    // Mappings, sources and sinks are all keyed by their name.
    @Valid public final ImmutableMap<String,MappingConfiguration> mappings;
    @Valid public final ImmutableMap<String,SourceConfiguration> sources;
    @Valid public final ImmutableMap<String,SinkConfiguration> sinks;

    @JsonCreator
    DivolteConfiguration(final GlobalConfiguration global,
                         final Optional<ImmutableMap<String,SourceConfiguration>> sources,
                         final Optional<ImmutableMap<String,SinkConfiguration>> sinks,
                         final Optional<ImmutableMap<String,MappingConfiguration>> mappings) {
        this.global = Objects.requireNonNull(global);
        this.sources = sources.orElseGet(DivolteConfiguration::defaultSourceConfigurations);
        this.sinks = sinks.orElseGet(DivolteConfiguration::defaultSinkConfigurations);
        this.mappings = mappings.orElseGet(() -> defaultMappingConfigurations(this.sources.keySet(), this.sinks.keySet()));
    }

    /*
     * This performs a linear search over the map. Only use in startup code;
     * avoid in inner loops.
     */
    private static <T> int position(final T key, final ImmutableMap<T,?> map) {
        final ImmutableList<T> keyList = map.keySet().asList();
        return keyList.indexOf(key);
    }

    /**
     * This performs a linear search over the map. Only use in startup code;
     * avoid in inner loops.
     */
    public int sourceIndex(final String name) {
        return position(name, sources);
    }

    /**
     * This performs a linear search over the map. Only use in startup code;
     * avoid in inner loops.
     */
    public int sinkIndex(final String name) {
        return position(name, sinks);
    }

    /**
     * This performs a linear search over the map. Only use in startup code;
     * avoid in inner loops.
     */
    public int mappingIndex(final String name) {
        return position(name, mappings);
    }

    /**
     * Retrieve the configuration for the source with the given name, casting it to an expected type.
     *
     * It is an error to request a source that doesn't exist or is of the wrong type: the caller is
     * responsible for knowing the name is valid and the type of source.
     *
     * @param sourceName    the name of the source whose configuration should be retrieved.
     * @param sourceClass   the class of the source configuration to retrieve.
     * @param <T>           the type of the source configuration to retrieve.
     * @return              the configuration for the given source.
     * @throws              IllegalArgumentException
     *                      if no configuration exists for the given source or its type is different
     *                      to that expected.
     */
    public <T> T getSourceConfiguration(final String sourceName, final Class <? extends T> sourceClass) {
        final SourceConfiguration sourceConfiguration = sources.get(sourceName);
        Preconditions.checkArgument(null != sourceConfiguration, "No source configuration with name: %s", sourceName);
        Preconditions.checkArgument(sourceClass.isInstance(sourceConfiguration),
                "Source configuration '%s' is not a %s sink", sourceName, sourceClass.getSimpleName());
        return sourceClass.cast(sourceConfiguration);
    }

    /**
     * Retrieve the configuration for the mapping with the given name.
     *
     * It is an error to request a mapping that doesn't exist: the caller is responsible for knowing
     * the name is valid.
     *
     * @param mappingName   the name of the mapping whose configuration should be retrieved.
     * @return              the configuration for the given mapping.
     * @throws              IllegalArgumentException
     *                      if no configuration exists for the given mapping.
     */
    public MappingConfiguration getMappingConfiguration(final String mappingName) {
        final MappingConfiguration mappingConfiguration = mappings.get(mappingName);
        Preconditions.checkArgument(null != mappingConfiguration, "No mapping configuration with name: %s", mappingName);
        return mappingConfiguration;
    }

    /**
     * Retrieve the configuration for the sink with the given name, casting it to an expected type.
     *
     * It is an error to request a sink that doesn't exist or is of the wrong type: the caller is
     * responsible for knowing the name is valid and the type of sink.
     *
     * @param sinkName  the name of the sink whose configuration should be retrieved.
     * @param sinkClass the class of the sink configuration to retrieve.
     * @param <T>       the type of the sink configuration to retrieve.
     * @return          the configuration for the given sink.
     * @throws          IllegalArgumentException
     *                  if no configuration exists for the given sink or its type is different
     *                  to that expected.
     */
    public <T> T getSinkConfiguration(final String sinkName, final Class <? extends T> sinkClass) {
        final SinkConfiguration sinkConfiguration = sinks.get(sinkName);
        Preconditions.checkArgument(null != sinkConfiguration, "No sink configuration with name: %s", sinkName);
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
