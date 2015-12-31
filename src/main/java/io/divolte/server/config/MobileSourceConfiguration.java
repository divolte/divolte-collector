/*
 * Copyright 2015 GoDataDriven B.V.
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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.divolte.server.MobileSource;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class MobileSourceConfiguration extends SourceConfiguration {
    @JsonCreator
    MobileSourceConfiguration(@JsonProperty(defaultValue=DEFAULT_PREFIX) final String prefix) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        super(prefix);
    }

    @Override
    public SourceFactory getFactory() {
        return MobileSource::new;
    }
}
