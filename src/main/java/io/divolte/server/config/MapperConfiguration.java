/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
