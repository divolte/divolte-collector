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

package io.divolte.server;

import javax.annotation.ParametersAreNonnullByDefault;

import io.divolte.server.config.JsonSourceConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Methods;

@ParametersAreNonnullByDefault
public class JsonSource extends HttpSource {
    public static final String EVENT_SOURCE_NAME = "json";

    private final JsonEventHandler handler;

    public JsonSource(final ValidatedConfiguration vc,
                        final String sourceName,
                        final IncomingRequestProcessingPool processingPool) {
        this(sourceName,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).prefix,
             processingPool,
             vc.configuration().sourceIndex(sourceName),
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).eventTypeParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).partyIdParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).sessionIdParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).eventIdParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).newPartyParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).newSessionParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).timeParameter
             );
    }

    private JsonSource(final String sourceName,
                         final String pathPrefix,
                         final IncomingRequestProcessingPool processingPool,
                         final int sourceIndex,
                         final String eventTypeParameter,
                         final String partyIdParameter,
                         final String sessionIdParameter,
                         final String eventIdParameter,
                         final String newPartyParameter,
                         final String newSessionParameter,
                         final String timeParameter
                         ) {
        super(sourceName, pathPrefix);
        this.handler = new JsonEventHandler(processingPool, sourceIndex, eventTypeParameter, partyIdParameter, sessionIdParameter,
                                            eventIdParameter, newPartyParameter, newSessionParameter, timeParameter);
    }

    @Override
    public PathHandler attachToPathHandler(final PathHandler pathHandler) {
        return pathHandler.addExactPath(pathPrefix,
                new AllowedMethodsHandler(          // Allow only POST for this endpoint
                        handler, Methods.POST));
    }
}
