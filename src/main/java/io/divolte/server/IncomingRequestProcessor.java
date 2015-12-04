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

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import io.divolte.record.DefaultEventRecord;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.hdfs.HdfsFlusher;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.kafka.KafkaFlusher;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ItemProcessor;
import io.divolte.server.processing.ProcessingPool;
import io.divolte.server.recordmapping.DslRecordMapper;
import io.divolte.server.recordmapping.DslRecordMapping;
import io.divolte.server.recordmapping.RecordMapper;
import io.divolte.server.recordmapping.UserAgentParserAndCache;
import io.undertow.util.AttachmentKey;

@ParametersAreNonnullByDefault
public final class IncomingRequestProcessor implements ItemProcessor<DivolteEvent> {
    public static final String INVALID_DATA_MARKER = "INVALID";
    public static final Marker invalid = MarkerFactory.getMarker(INVALID_DATA_MARKER);
    private static final Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    public static final AttachmentKey<Boolean> DUPLICATE_EVENT_KEY = AttachmentKey.create(Boolean.class);

    @Nullable
    private final ProcessingPool<KafkaFlusher, AvroRecordBuffer> kafkaFlushingPool;
    @Nullable
    private final ProcessingPool<HdfsFlusher, AvroRecordBuffer> hdfsFlushingPool;

    private final IncomingRequestListener listener;

    private final RecordMapper mapper;

    private final boolean keepCorrupted;

    private final ShortTermDuplicateMemory memory;
    private final boolean keepDuplicates;

    public IncomingRequestProcessor(final ValidatedConfiguration vc,
                                    @Nullable final KafkaFlushingPool kafkaFlushingPool,
                                    @Nullable final HdfsFlushingPool hdfsFlushingPool,
                                    @Nullable final LookupService geoipLookupService,
                                    final Schema schema,
                                    final IncomingRequestListener listener) {

        this.kafkaFlushingPool = kafkaFlushingPool;
        this.hdfsFlushingPool = hdfsFlushingPool;
        this.listener = Objects.requireNonNull(listener);

        keepCorrupted = !vc.configuration().incomingRequestProcessor.discardCorrupted;

        memory = new ShortTermDuplicateMemory(vc.configuration().incomingRequestProcessor.duplicateMemorySize);
        keepDuplicates = !vc.configuration().incomingRequestProcessor.discardDuplicates;

        mapper = vc.configuration().tracking.schemaMapping
            .map((smc) -> {
                final int version = smc.version;
                switch(version) {
                case 1:
                    logger.error("Version 1 configuration version had been deprecated and is no longer supported.");
                    throw new RuntimeException("Unsupported schema mapping config version: " + version);
                case 2:
                    logger.info("Using script based schema mapping.");
                    return new DslRecordMapper(
                            vc,
                            Objects.requireNonNull(schema),
                            Optional.ofNullable(geoipLookupService));
                default:
                    throw new RuntimeException("Unsupported schema mapping config version: " + version);
                }
            })
            .orElseGet(() -> {
                logger.info("Using built in default schema mapping.");
                return new DslRecordMapper(DefaultEventRecord.getClassSchema(), defaultRecordMapping(vc));
            });
    }

    private DslRecordMapping defaultRecordMapping(final ValidatedConfiguration vc) {
        final DslRecordMapping result = new DslRecordMapping(DefaultEventRecord.getClassSchema(), new UserAgentParserAndCache(vc), Optional.empty());
        result.map("detectedCorruption", result.corrupt());
        result.map("detectedDuplicate", result.duplicate());
        result.map("firstInSession", result.firstInSession());
        result.map("timestamp", result.timestamp());
        result.map("clientTimestamp", result.clientTimestamp());
        result.map("remoteHost", result.remoteHost());
        result.map("referer", result.referer());
        result.map("location", result.location());
        result.map("viewportPixelWidth", result.viewportPixelWidth());
        result.map("viewportPixelHeight", result.viewportPixelHeight());
        result.map("screenPixelWidth", result.screenPixelWidth());
        result.map("screenPixelHeight", result.screenPixelHeight());
        result.map("partyId", result.partyId());
        result.map("sessionId", result.sessionId());
        result.map("pageViewId", result.pageViewId());
        result.map("eventType", result.eventType());
        result.map("userAgentString", result.userAgentString());
        final DslRecordMapping.UserAgentValueProducer userAgent = result.userAgent();
        result.map("userAgentName", userAgent.name());
        result.map("userAgentFamily", userAgent.family());
        result.map("userAgentVendor", userAgent.vendor());
        result.map("userAgentType", userAgent.type());
        result.map("userAgentVersion", userAgent.version());
        result.map("userAgentDeviceCategory", userAgent.deviceCategory());
        result.map("userAgentOsFamily", userAgent.osFamily());
        result.map("userAgentOsVersion", userAgent.osVersion());
        result.map("userAgentOsVendor", userAgent.osVendor());
        return result;
    }

    @Override
    public ProcessingDirective process(final DivolteEvent event) {
        if (!event.corruptEvent || keepCorrupted) {
            /*
             * Note: we cannot use the actual query string here,
             * as the incoming request processor is agnostic of
             * that sort of thing. The request may have come from
             * an endpoint that doesn't require a query string,
             * but rather generates these IDs on the server side.
             */
            final boolean duplicate = memory.isProbableDuplicate(event.partyCookie.value, event.sessionCookie.value, event.eventId);
            event.exchange.putAttachment(DUPLICATE_EVENT_KEY, duplicate);

            if (!duplicate || keepDuplicates) {
                final GenericRecord avroRecord = mapper.newRecordFromExchange(event);
                try {
                    final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(
                        event.partyCookie,
                        event.sessionCookie,
                        event.requestStartTime,
                        event.clientUtcOffset,
                        avroRecord);
                    listener.incomingRequest(event, avroBuffer, avroRecord);
                    doProcess(avroBuffer);
                } catch (final RuntimeException e) {
                    logger.warn(invalid, "Error processing event {} from party {} in session {}: {}",
                                event, event.partyCookie, event.sessionCookie, avroRecord, e);
                }
            }
        }

        return CONTINUE;
    }

    private void doProcess(final AvroRecordBuffer avroBuffer) {

        if (null != kafkaFlushingPool) {
            kafkaFlushingPool.enqueue(avroBuffer.getPartyId().value, avroBuffer);
        }
        if (null != hdfsFlushingPool) {
            hdfsFlushingPool.enqueue(avroBuffer.getPartyId().value, avroBuffer);
        }
    }
}
