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

import io.divolte.server.HttpSource;
import io.divolte.server.IncomingRequestProcessingPool;
import io.divolte.server.JsonSource;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class JsonSourceConfiguration extends SourceConfiguration {
    public final static String DEFAULT_EVENT_TYPE_PARAMETER = "t";
    public final static String DEFAULT_PARTY_ID_PARAMETER = "p";
    public final static String DEFAULT_SESSION_ID_PARAMETER = "s";
    public final static String DEFAULT_EVENT_ID_PARAMETER = "e";
    public final static String DEFAULT_TIME_PARAMETER = "c";
    public final static String DEFAULT_NEW_PARTY_PARAMETER = "np";
    public final static String DEFAULT_NEW_SESSION_PARAMETER = "ns";

    public final String eventTypeParameter;
    public final String partyIdParameter;
    public final String sessionIdParameter;
    public final String eventIdParameter;
    public final String newPartyParameter;
    public final String newSessionParameter;
    public final String timeParameter;

    @JsonCreator
    JsonSourceConfiguration(
            @JsonProperty(defaultValue=DEFAULT_PREFIX) final String prefix,
            @JsonProperty(defaultValue=DEFAULT_EVENT_TYPE_PARAMETER) final String eventTypeParameter,
            @JsonProperty(defaultValue=DEFAULT_PARTY_ID_PARAMETER) final String partyIdParameter,
            @JsonProperty(defaultValue=DEFAULT_SESSION_ID_PARAMETER) final String sessionIdParameter,
            @JsonProperty(defaultValue=DEFAULT_EVENT_ID_PARAMETER) final String eventIdParameter,
            @JsonProperty(defaultValue=DEFAULT_NEW_PARTY_PARAMETER) final String newPartyParameter,
            @JsonProperty(defaultValue=DEFAULT_NEW_SESSION_PARAMETER) final String newSessionParameter,
            @JsonProperty(defaultValue=DEFAULT_TIME_PARAMETER) final String timeParameter) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        super(prefix);
        this.eventTypeParameter = eventTypeParameter == null ? DEFAULT_EVENT_TYPE_PARAMETER : eventTypeParameter;
        this.partyIdParameter = partyIdParameter == null ? DEFAULT_PARTY_ID_PARAMETER : partyIdParameter;
        this.sessionIdParameter = sessionIdParameter == null ? DEFAULT_SESSION_ID_PARAMETER : sessionIdParameter;
        this.eventIdParameter = eventIdParameter == null ? DEFAULT_EVENT_ID_PARAMETER : eventIdParameter;
        this.newPartyParameter = newPartyParameter == null ? DEFAULT_NEW_PARTY_PARAMETER : newPartyParameter;
        this.newSessionParameter = newSessionParameter == null ? DEFAULT_NEW_SESSION_PARAMETER : newSessionParameter;
        this.timeParameter = timeParameter == null ? DEFAULT_TIME_PARAMETER : timeParameter;
    }

    @Override
    public HttpSource createSource(
            final ValidatedConfiguration vc,
            final String sourceName,
            final IncomingRequestProcessingPool processingPool) {
        return new JsonSource(vc, sourceName, processingPool);
    }
}
