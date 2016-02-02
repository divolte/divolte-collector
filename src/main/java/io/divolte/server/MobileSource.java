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

import io.divolte.server.config.MobileSourceConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.undertow.server.handlers.PathHandler;

@ParametersAreNonnullByDefault
public class MobileSource extends HttpSource {
    public MobileSource(final ValidatedConfiguration vc,
                        final String sourceName,
                        final IncomingRequestProcessingPool processingPool) {
        this(sourceName,
             vc.configuration().getSourceConfiguration(sourceName, MobileSourceConfiguration.class).prefix,
             processingPool);
    }

    private MobileSource(final String sourceName,
                         final String pathPrefix,
                         @SuppressWarnings("unused") final IncomingRequestProcessingPool processingPool) {
        super(sourceName, pathPrefix);
        // TODO: Implement me.
    }

    @Override
    public PathHandler attachToPathHandler(final PathHandler pathHandler) {
        // TODO: Implement me.
        return pathHandler;
    }
}