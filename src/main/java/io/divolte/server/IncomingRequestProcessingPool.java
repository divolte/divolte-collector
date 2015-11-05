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

import io.divolte.record.DefaultEventRecord;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.ip2geo.ExternalDatabaseLookupService;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ProcessingPool;

@ParametersAreNonnullByDefault
final class IncomingRequestProcessingPool extends ProcessingPool<IncomingRequestProcessor, DivolteEvent> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessingPool.class);

    private final Optional<KafkaFlushingPool> kafkaPool;
    private final Optional<HdfsFlushingPool> hdfsPool;

    public IncomingRequestProcessingPool(final ValidatedConfiguration vc, final IncomingRequestListener listener) {
        this (
                vc.configuration().global.mapper.threads,
                vc.configuration().global.mapper.bufferSize,
                vc,
                schemaFromConfig(vc),
                vc.configuration().global.kafka.enabled ? new KafkaFlushingPool(vc) : null,
                vc.configuration().global.hdfs.enabled ? new HdfsFlushingPool(vc, schemaFromConfig(vc)) : null,
                lookupServiceFromConfig(vc),
                listener
                );
    }

    public IncomingRequestProcessingPool(
            final int numThreads,
            final int maxQueueSize,
            final ValidatedConfiguration vc,
            final Schema schema,
            @Nullable final KafkaFlushingPool kafkaFlushingPool,
            @Nullable final HdfsFlushingPool hdfsFlushingPool,
            @Nullable final LookupService geoipLookupService,
            final IncomingRequestListener listener) {
        super(
                numThreads,
                maxQueueSize,
                "Incoming Request Processor",
                () -> new IncomingRequestProcessor(vc, kafkaFlushingPool, hdfsFlushingPool, geoipLookupService, schema, listener));

        this.kafkaPool = Optional.ofNullable(kafkaFlushingPool);
        this.hdfsPool = Optional.ofNullable(hdfsFlushingPool);
    }

    private static Schema schemaFromConfig(final ValidatedConfiguration vc) {
        return vc.configuration().incomingRequestProcessor.schemaFile
            .map((schemaFileName) -> {
                final Parser parser = new Schema.Parser();
                logger.info("Using Avro schema from configuration: {}", schemaFileName);
                try {
                    return parser.parse(new File(schemaFileName));
                } catch(final IOException ioe) {
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
        return vc.configuration().global.mapper.ip2geoDatabase
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

    public void enqueueIncomingExchangeForProcessing(final DivolteIdentifier partyId, final DivolteEvent event) {
        enqueue(partyId.value, event);
    }

    @Override
    public void stop() {
        super.stop();

        kafkaPool.ifPresent(KafkaFlushingPool::stop);
        hdfsPool.ifPresent(HdfsFlushingPool::stop);
    }
}
