/*
 * Copyright 2014 GoDataDriven B.V.
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
import java.nio.file.Paths;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.processing.ProcessingPool;

@ParametersAreNonnullByDefault
public final class IncomingRequestProcessingPool extends ProcessingPool<IncomingRequestProcessor, UndertowEvent> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    public IncomingRequestProcessingPool(final ValidatedConfiguration vc,
                                         final SchemaRegistry schemaRegistry,
                                         final ImmutableMap<String, ProcessingPool<?, AvroRecordBuffer>> sinksByName,
                                         final IncomingRequestListener listener) {
        this (
                vc,
                schemaRegistry,
                sinksByName,
                lookupServiceFromConfig(vc),
                listener
                );
    }

    public IncomingRequestProcessingPool(
            final ValidatedConfiguration vc,
            final SchemaRegistry schemaRegistry,
            final ImmutableMap<String, ProcessingPool<?, AvroRecordBuffer>> sinksByName,
            final Optional<LookupService> geoipLookupService,
            final IncomingRequestListener listener) {
        super(
                vc.configuration().global.mapper.threads,
                vc.configuration().global.mapper.bufferSize,
                "Incoming Request Processor",
                () -> new IncomingRequestProcessor(vc, sinksByName, geoipLookupService, schemaRegistry, listener));
    }

    private static Optional<LookupService> lookupServiceFromConfig(final ValidatedConfiguration vc) {
        // XXX: This service should be a singleton, instead of per-pool.
        return vc.configuration().global.mapper.ip2geoDatabase
            .map((path) -> {
                try {
                    return new ExternalDatabaseLookupService(Paths.get(path));
                } catch (final IOException e) {
                    logger.error("Failed to configure GeoIP database: " + path, e);
                    throw new RuntimeException("Failed to configure GeoIP lookup service.", e);
                }
            });
    }
}
