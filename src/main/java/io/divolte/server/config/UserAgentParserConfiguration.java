/*
 * Copyright 2017 GoDataDriven B.V.
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
