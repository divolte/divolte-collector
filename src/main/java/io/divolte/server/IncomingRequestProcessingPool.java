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

import io.divolte.record.DefaultEventRecord;
import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ProcessingPool;
import io.undertow.server.HttpServerExchange;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessingPool extends ProcessingPool<IncomingRequestProcessor, HttpServerExchange> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    private final Optional<KafkaFlushingPool> kafkaPool;
    private final Optional<HdfsFlushingPool> hdfsPool;

    public IncomingRequestProcessingPool(final ValidatedConfiguration vc, IncomingRequestListener listener) {
        this (
                vc.configuration().incomingRequestProcessor.threads,
                vc.configuration().incomingRequestProcessor.maxWriteQueue,
                vc.configuration().incomingRequestProcessor.maxEnqueueDelay.toMillis(),
                vc,
                schemaFromConfig(vc),
                vc.configuration().kafkaFlusher.enabled ? new KafkaFlushingPool(vc) : null,
                vc.configuration().hdfsFlusher.enabled ? new HdfsFlushingPool(vc, schemaFromConfig(vc)) : null,
                lookupServiceFromConfig(vc),
                listener
                );
    }

    public IncomingRequestProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final long maxEnqueueDelay,
            final ValidatedConfiguration vc,
            final Schema schema,
            @Nullable final KafkaFlushingPool kafkaFlushingPool,
            @Nullable final HdfsFlushingPool hdfsFlushingPool,
            @Nullable final LookupService geoipLookupService,
            final IncomingRequestListener listener) {
        super(
                numThreads,
                maxQueueSize,
                maxEnqueueDelay,
                "Incoming Request Processor",
                () -> new IncomingRequestProcessor(vc, kafkaFlushingPool, hdfsFlushingPool, geoipLookupService, schema, listener));

        this.kafkaPool = Optional.ofNullable(kafkaFlushingPool);
        this.hdfsPool = Optional.ofNullable(hdfsFlushingPool);
    }

    private static Schema schemaFromConfig(final ValidatedConfiguration vc) {
        return vc.configuration().tracking.schemaFile
            .map((schemaFileName) -> {
                final Parser parser = new Schema.Parser();
                logger.info("Using Avro schema from configuration: {}", schemaFileName);
                try {
                    return parser.parse(new File(schemaFileName));
                } catch(IOException ioe) {
                    logger.error("Failed to load Avro schema file.");
                    throw new RuntimeException("Failed to load Avro schema file.", ioe);
                }
            })
            .orElseGet(() -> {
                logger.info("Using built in default Avro schema.");
                return DefaultEventRecord.getClassSchema();
            });
    }

    @Nullable
    private static LookupService lookupServiceFromConfig(final ValidatedConfiguration vc) {
        return vc.configuration().tracking.ip2geoDatabase
            .map((path) -> {
                try {
                    return new ExternalDatabaseLookupService(Paths.get(path));
                } catch (final IOException e) {
                    logger.error("Failed to configure GeoIP database: " + path, e);
                    throw new RuntimeException("Failed to configure GeoIP lookup service.", e);
                }
            })
            .orElse(null);
    }

    public void enqueueIncomingExchangeForProcessing(final CookieValue partyId, final HttpServerExchange exchange) {
        enqueue(partyId.value, exchange);
    }

    @Override
    public void stop() {
        super.stop();

        kafkaPool.ifPresent(KafkaFlushingPool::stop);
        hdfsPool.ifPresent(HdfsFlushingPool::stop);
    }
}
