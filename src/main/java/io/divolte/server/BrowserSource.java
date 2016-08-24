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

import java.io.IOException;
import java.io.UncheckedIOException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.divolte.server.config.BrowserSourceConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.js.TrackingJavaScriptResource;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Methods;

@ParametersAreNonnullByDefault
public class BrowserSource extends HttpSource {
    private static final Logger logger = LoggerFactory.getLogger(BrowserSource.class);

    private final String pathPrefix;
    private final String eventSuffix;
    private final String javascriptName;
    private final HttpHandler javascriptHandler;
    private final HttpHandler eventHandler;

    public static final String EVENT_SOURCE_NAME = "browser";

    public BrowserSource(final ValidatedConfiguration vc,
                         final String sourceName,
                         final IncomingRequestProcessingPool processingPool) {
        this(sourceName,
             vc.configuration().getSourceConfiguration(sourceName, BrowserSourceConfiguration.class).prefix,
             vc.configuration().getSourceConfiguration(sourceName, BrowserSourceConfiguration.class).eventSuffix,
             loadTrackingJavaScript(vc, sourceName),
             processingPool,
             vc.configuration().sourceIndex(sourceName));
    }

    private BrowserSource(final String sourceName,
                          final String pathPrefix,
                          final String eventSuffix,
                          final TrackingJavaScriptResource trackingJavascript,
                          final IncomingRequestProcessingPool processingPool,
                          final int sourceIndex) {
        super(sourceName);
        this.pathPrefix = pathPrefix;
        this.eventSuffix = eventSuffix;
        javascriptName = trackingJavascript.getScriptName();
        javascriptHandler = new AllowedMethodsHandler(new JavaScriptHandler(trackingJavascript), Methods.GET);
        final ClientSideCookieEventHandler clientSideCookieEventHandler = new ClientSideCookieEventHandler(processingPool, sourceIndex);
        eventHandler = new AllowedMethodsHandler(clientSideCookieEventHandler, Methods.GET);
    }

    @Override
    public PathHandler attachToPathHandler(PathHandler pathHandler) {
        final String javascriptPath = pathPrefix + javascriptName;
        pathHandler = pathHandler.addExactPath(javascriptPath, javascriptHandler);
        logger.info("Registered source[{}] script location: {}", sourceName, javascriptPath);
        final String eventPath = pathPrefix + eventSuffix;
        pathHandler = pathHandler.addExactPath(eventPath, eventHandler);
        logger.info("Registered source[{}] event handler: {}", sourceName, eventPath);
        return pathHandler;
    }

    private static TrackingJavaScriptResource loadTrackingJavaScript(final ValidatedConfiguration vc, final String sourceName) {
        try {
            return TrackingJavaScriptResource.create(vc, sourceName);
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not precompile tracking JavaScript for source: " + sourceName, e);
        }
    }
}
