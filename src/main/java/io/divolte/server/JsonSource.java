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

import io.divolte.server.config.JsonSourceConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class JsonSource extends HttpSource {
    private static final Logger logger = LoggerFactory.getLogger(JsonSource.class);
    public static final String EVENT_SOURCE_NAME = "json";

    private final String eventPath;
    private final JsonEventHandler handler;

    public JsonSource(final ValidatedConfiguration vc,
                      final String sourceName,
                      final IncomingRequestProcessingPool processingPool) {
        this(sourceName,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).eventPath,
             processingPool,
             vc.configuration().sourceIndex(sourceName),
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).partyIdParameter,
             vc.configuration().getSourceConfiguration(sourceName, JsonSourceConfiguration.class).maximumBodySize);
    }

    private JsonSource(final String sourceName,
                         final String eventPath,
                         final IncomingRequestProcessingPool processingPool,
                         final int sourceIndex,
                         final String partyIdParameter,
                         final int maximumBodySize) {
        super(sourceName);
        this.eventPath = eventPath;
        this.handler = new JsonEventHandler(processingPool, sourceIndex, partyIdParameter, maximumBodySize);
    }

    @Override
    public PathHandler attachToPathHandler(final PathHandler pathHandler) {
        final HttpHandler onlyJsonHandler = new JsonContentHandler(handler);
        final HttpHandler onlyPostHandler = new AllowedMethodsHandler(onlyJsonHandler, Methods.POST);
        final PathHandler newPathHandler = pathHandler.addExactPath(eventPath, onlyPostHandler);
        logger.info("Registered source[{}] event handler: {}", sourceName, eventPath);
        return newPathHandler;
    }
}
