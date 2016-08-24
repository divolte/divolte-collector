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

import com.google.common.base.MoreObjects;
import io.divolte.server.HttpSource;
import io.divolte.server.IncomingRequestProcessingPool;
import io.divolte.server.JsonSource;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class JsonSourceConfiguration extends SourceConfiguration {
    private final static String DEFAULT_EVENT_PATH = "/";
    private final static String DEFAULT_PARTY_ID_PARAMETER = "p";
    public final static String DEFAULT_MAXIMUM_BODY_SIZE = "4096";

    public final String eventPath;
    public final String partyIdParameter;
    public final int maximumBodySize;

    @JsonCreator
    @ParametersAreNullableByDefault
    JsonSourceConfiguration(
            @JsonProperty(defaultValue=DEFAULT_EVENT_PATH) final String eventPath,
            @JsonProperty(defaultValue=DEFAULT_PARTY_ID_PARAMETER) final String partyIdParameter,
            @JsonProperty(defaultValue=DEFAULT_MAXIMUM_BODY_SIZE) final Integer maximumBodySize) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.eventPath = Optional.ofNullable(eventPath).orElse(DEFAULT_EVENT_PATH);
        this.partyIdParameter = Optional.ofNullable(partyIdParameter).orElse(DEFAULT_PARTY_ID_PARAMETER);
        this.maximumBodySize = Optional.ofNullable(maximumBodySize).orElseGet(() -> Integer.valueOf(DEFAULT_MAXIMUM_BODY_SIZE));
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("eventPath", eventPath)
            .add("partyIdParameter", partyIdParameter)
            .add("maximumBodySize", maximumBodySize);
    }

    @Override
    public HttpSource createSource(
            final ValidatedConfiguration vc,
            final String sourceName,
            final IncomingRequestProcessingPool processingPool) {
        return new JsonSource(vc, sourceName, processingPool);
    }
}
