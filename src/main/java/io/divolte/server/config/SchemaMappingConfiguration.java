package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class SchemaMappingConfiguration {
    public final int version;
    public final String mappingScriptFile;

    @JsonCreator
    private SchemaMappingConfiguration(final int version, final String mappingScriptFile) {
        this.version = version;
        this.mappingScriptFile = Objects.requireNonNull(mappingScriptFile);
    }

    @Override
    public String toString() {
        return "SchemaMappingConfiguration [version=" + version + ", mappingScriptFile=" + mappingScriptFile + "]";
    }
}
