package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class MappingConfiguration {
    private static final String DEFAULT_DISCARD_CORRUPTED = "false";
    private static final String DEFAULT_DISCARD_DUPLICATES = "false";

    public final Optional<String> schemaFile;
    public final Optional<String> mappingScriptFile;

    public final ImmutableSet<String> sources;
    public final ImmutableSet<String> sinks;

    public final boolean discardCorrupted;
    public final boolean discardDuplicates;

    @JsonCreator
    MappingConfiguration(final Optional<String> schemaFile,
                         final Optional<String> mappingScriptFile,
                         @JsonProperty(required = true)
                         final ImmutableSet<String> sources,
                         @JsonProperty(required = true)
                         final ImmutableSet<String> sinks,
                         @JsonProperty(defaultValue=DEFAULT_DISCARD_CORRUPTED)
                         @Nullable
                         final Boolean discardCorrupted,
                         @JsonProperty(defaultValue=DEFAULT_DISCARD_DUPLICATES)
                         @Nullable
                         final Boolean discardDuplicates) {
        this.schemaFile = Objects.requireNonNull(schemaFile);
        this.mappingScriptFile = Objects.requireNonNull(mappingScriptFile);
        this.sources = Objects.requireNonNull(sources);
        this.sinks = Objects.requireNonNull(sinks);
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.discardCorrupted = Optional.ofNullable(discardCorrupted).orElseGet(() -> Boolean.valueOf(DEFAULT_DISCARD_CORRUPTED));
        this.discardDuplicates = Optional.ofNullable(discardDuplicates).orElseGet(() -> Boolean.valueOf(DEFAULT_DISCARD_DUPLICATES));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("schemaFile", schemaFile)
                .add("mappingScriptFile", mappingScriptFile)
                .add("sources", sources)
                .add("sinks", sinks)
                .add("discardCorrupted", discardCorrupted)
                .add("discardDuplicates", discardDuplicates)
                .toString();
    }
}
