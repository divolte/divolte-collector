package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class UaParserConfiguration {
    public final String type;
    public final int cacheSize;

    @JsonCreator
    private UaParserConfiguration(final String type, final int cacheSize) {
        this.type = Objects.requireNonNull(type);
        this.cacheSize = cacheSize;
    }

    @Override
    public String toString() {
        return "UaParserConfiguration [type=" + type + ", cacheSize=" + cacheSize + "]";
    }
}
