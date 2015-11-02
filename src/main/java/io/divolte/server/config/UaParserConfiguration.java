package io.divolte.server.config;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class UaParserConfiguration {
    public final String type;
    public final int cacheSize;

    @JsonCreator
    private UaParserConfiguration(final String type, final int cacheSize) {
        this.type = type;
        this.cacheSize = cacheSize;
    }

    @Override
    public String toString() {
        return "UaParserConfiguration [type=" + type + ", cacheSize=" + cacheSize + "]";
    }
}
