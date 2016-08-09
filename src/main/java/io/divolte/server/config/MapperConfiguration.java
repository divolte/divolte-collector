package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class MapperConfiguration {
    public final int bufferSize;
    public final int threads;
    public final int duplicateMemorySize;
    public final UserAgentParserConfiguration userAgentParser;
    public final Optional<String> ip2geoDatabase;

    @JsonCreator
    MapperConfiguration(final int bufferSize,
                        final int threads,
                        final int duplicateMemorySize,
                        final UserAgentParserConfiguration userAgentParser,
                        final Optional<String> ip2geoDatabase) {
        this.bufferSize = bufferSize;
        this.threads = threads;
        this.duplicateMemorySize = duplicateMemorySize;
        this.userAgentParser = Objects.requireNonNull(userAgentParser);
        this.ip2geoDatabase = Objects.requireNonNull(ip2geoDatabase);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("bufferSize", bufferSize)
                .add("threads", threads)
                .add("duplicateMemorySize", duplicateMemorySize)
                .add("userAgentParser", userAgentParser)
                .add("ip2geoDatabase", ip2geoDatabase)
                .toString();
    }
}
