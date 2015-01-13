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

import static io.divolte.server.BaseEventHandler.*;
import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;
import io.divolte.record.DefaultEventRecord;
import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.hdfs.HdfsFlusher;
import io.divolte.server.hdfs.HdfsFlushingPool;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.kafka.KafkaFlusher;
import io.divolte.server.kafka.KafkaFlushingPool;
import io.divolte.server.processing.ItemProcessor;
import io.divolte.server.processing.ProcessingPool;
import io.divolte.server.recordmapping.ConfigRecordMapper;
import io.divolte.server.recordmapping.DslRecordMapper;
import io.divolte.server.recordmapping.DslRecordMapping;
import io.divolte.server.recordmapping.RecordMapper;
import io.divolte.server.recordmapping.UserAgentParserAndCache;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
public final class IncomingRequestProcessor implements ItemProcessor<HttpServerExchange> {
    private final static Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    public final static AttachmentKey<Boolean> CORRUPT_EVENT_KEY = AttachmentKey.create(Boolean.class);
    public final static AttachmentKey<Boolean> DUPLICATE_EVENT_KEY = AttachmentKey.create(Boolean.class);

    @Nullable
    private final ProcessingPool<KafkaFlusher, AvroRecordBuffer> kafkaFlushingPool;
    @Nullable
    private final ProcessingPool<HdfsFlusher, AvroRecordBuffer> hdfsFlushingPool;

    private final IncomingRequestListener listener;

    private final RecordMapper mapper;

    private final boolean keepCorrupted;

    private final ShortTermDuplicateMemory memory;
    private final boolean keepDuplicates;

    public IncomingRequestProcessor(final Config config,
                                    @Nullable final KafkaFlushingPool kafkaFlushingPool,
                                    @Nullable final HdfsFlushingPool hdfsFlushingPool,
                                    @Nullable final LookupService geoipLookupService,
                                    final Schema schema,
                                    final IncomingRequestListener listener) {

        this.kafkaFlushingPool = kafkaFlushingPool;
        this.hdfsFlushingPool = hdfsFlushingPool;
        this.listener = listener;

        keepCorrupted = !config.getBoolean("divolte.incoming_request_processor.discard_corrupted");

        memory = new ShortTermDuplicateMemory(config.getInt("divolte.incoming_request_processor.duplicate_memory_size"));
        keepDuplicates = !config.getBoolean("divolte.incoming_request_processor.discard_duplicates");

        if (config.hasPath("divolte.tracking.schema_mapping")) {
            final int version = config.getInt("divolte.tracking.schema_mapping.version");
            switch(version) {
            case 1:
                logger.info("Using configuration based schema mapping.");
                mapper = new ConfigRecordMapper(
                        Objects.requireNonNull(schema),
                        config,
                        Optional.ofNullable(geoipLookupService));
                break;
            case 2:
                logger.info("Using script based schema mapping.");
                mapper = new DslRecordMapper(
                        config,
                        Objects.requireNonNull(schema),
                        Optional.ofNullable(geoipLookupService));
                break;
            default:
                throw new RuntimeException("Unsupported schema mapping config version: " + version);
            }
        } else {
            logger.info("Using built in default schema mapping.");
            mapper = new DslRecordMapper(DefaultEventRecord.getClassSchema(), defaultRecordMapping(config));
        }
    }

    private DslRecordMapping defaultRecordMapping(final Config config) {
        final DslRecordMapping result = new DslRecordMapping(DefaultEventRecord.getClassSchema(), new UserAgentParserAndCache(config), Optional.empty());
        result.map("detectedCorruption", result.corrupt());
        result.map("detectedDuplicate", result.duplicate());
        result.map("firstInSession", result.firstInSession());
        result.map("timestamp", result.timestamp());
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
    public ProcessingDirective process(final HttpServerExchange exchange) {
        final boolean corrupt = exchange.getAttachment(CORRUPT_EVENT_KEY);

        if (!corrupt || keepCorrupted) {
            final CookieValue party = exchange.getAttachment(PARTY_COOKIE_KEY);
            final CookieValue session = exchange.getAttachment(SESSION_COOKIE_KEY);
            final String pageView = exchange.getAttachment(PAGE_VIEW_ID_KEY);
            final String event = exchange.getAttachment(EVENT_ID_KEY);
            final Long requestStartTime = exchange.getAttachment(REQUEST_START_TIME_KEY);
            final Long cookieUtcOffset = exchange.getAttachment(COOKIE_UTC_OFFSET_KEY);

            /*
             * Note: we cannot use the actual query string here,
             * as the incoming request processor is agnostic of
             * that sort of thing. The request may have come from
             * an endpoint that doesn't require a query string,
             * but rather generates these IDs on the server side.
             */
            final boolean duplicate = memory.isProbableDuplicate(party.value, session.value, pageView, event);
            exchange.putAttachment(DUPLICATE_EVENT_KEY, duplicate);

            if (!duplicate || keepDuplicates) {
                final GenericRecord avroRecord = mapper.newRecordFromExchange(exchange);
                final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(
                        party,
                        session,
                        requestStartTime,
                        cookieUtcOffset,
                        avroRecord);
                doProcess(exchange, avroRecord, avroBuffer);
            }
        }

        return CONTINUE;
    }

    private void doProcess(final HttpServerExchange exchange, final GenericRecord avroRecord, final AvroRecordBuffer avroBuffer) {
        listener.incomingRequest(exchange, avroBuffer, avroRecord);

        if (null != kafkaFlushingPool) {
            kafkaFlushingPool.enqueue(avroBuffer.getPartyId().value, avroBuffer);
        }
        if (null != hdfsFlushingPool) {
            hdfsFlushingPool.enqueue(avroBuffer.getPartyId().value, avroBuffer);
        }
    }
}
