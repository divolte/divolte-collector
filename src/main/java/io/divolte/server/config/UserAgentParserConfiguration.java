package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Locale;
import java.util.Objects;

@ParametersAreNonnullByDefault
public final class UserAgentParserConfiguration {
    public final ParserType type;
    public final int cacheSize;

    @JsonCreator
    UserAgentParserConfiguration(final ParserType type, final int cacheSize) {
        this.type = Objects.requireNonNull(type);
        this.cacheSize = cacheSize;
    }

    @Override
    public String toString() {
        return "UserAgentParserConfiguration [type=" + type + ", cacheSize=" + cacheSize + "]";
    }

    @ParametersAreNonnullByDefault
    public enum ParserType {
        NON_UPDATING,
        ONLINE_UPDATING,
        CACHING_AND_UPDATING;

        // Ensure that enumeration names are case-insensitive when parsing JSON.
        @JsonCreator
        static ParserType fromJson(final String value) {
            return ParserType.valueOf(value.toUpperCase(Locale.ROOT));
        }
    }
}
