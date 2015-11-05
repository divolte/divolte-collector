package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class MappingConfiguration {
    private static final boolean DEFAULT_DISCARD_CORRUPTED = false;
    private static final boolean DEFAULT_DISCARD_DUPLICATES = false;

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
                         final Optional<Boolean> discardCorrupted,
                         final Optional<Boolean> discardDuplicates) {
        this.schemaFile = Objects.requireNonNull(schemaFile);
        this.mappingScriptFile = Objects.requireNonNull(mappingScriptFile);
        this.sources = Objects.requireNonNull(sources);
        this.sinks = Objects.requireNonNull(sinks);
        this.discardCorrupted = discardCorrupted.orElse(DEFAULT_DISCARD_CORRUPTED);
        this.discardDuplicates = discardDuplicates.orElse(DEFAULT_DISCARD_DUPLICATES);
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
