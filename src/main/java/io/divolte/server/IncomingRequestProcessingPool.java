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

import com.google.common.collect.ImmutableSet;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.processing.ProcessingPool;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessingPool extends ProcessingPool<IncomingRequestProcessor, DivolteEvent> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    public IncomingRequestProcessingPool(final ValidatedConfiguration vc,
                                         final String name,
                                         final SchemaRegistry schemaRegistry,
                                         final Function<String, Optional<ProcessingPool<?, AvroRecordBuffer>>> sinkProvider,
                                         final IncomingRequestListener listener) {
        this (
                vc.configuration().global.mapper.threads,
                vc.configuration().global.mapper.bufferSize,
                vc,
                name,
                schemaRegistry.getSchemaByMappingName(name),
                buildSinksForwarder(sinkProvider, vc.configuration().mappings.get(name).sinks),
                lookupServiceFromConfig(vc),
                listener
                );
    }

    private static EventForwarder<AvroRecordBuffer> buildSinksForwarder(final Function<String, Optional<ProcessingPool<?, AvroRecordBuffer>>> sinkProvider,
                                                                        final ImmutableSet<String> sinkNames) {
        // Some sinks may not be available via the provider: these have been globally disabled.
        return EventForwarder.create(sinkNames.stream()
                                              .map(sinkProvider::apply)
                                              .filter(Optional::isPresent)
                                              .map(Optional::get)
                                              .collect(MoreCollectors.toImmutableList()));
    }

    public IncomingRequestProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final ValidatedConfiguration vc,
            final String name,
            final Schema schema,
            final EventForwarder<AvroRecordBuffer> flushingPools,
            final Optional<LookupService> geoipLookupService,
            final IncomingRequestListener listener) {
        super(
                numThreads,
                maxQueueSize,
                "Incoming Request Processor",
                () -> new IncomingRequestProcessor(vc, name, flushingPools, geoipLookupService, schema, listener));
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
