package io.divolte.server;

import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.divolte.record.DefaultEventRecord;
import io.divolte.server.config.MappingConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.processing.Item;
import io.divolte.server.recordmapping.DslRecordMapper;
import io.divolte.server.recordmapping.DslRecordMapping;
import io.divolte.server.recordmapping.UserAgentParserAndCache;

public class Mapping {
    private static final Logger logger = LoggerFactory.getLogger(Mapping.class);

    private final DslRecordMapper mapper;
    private final boolean keepCorrupted;
    private final boolean keepDuplicates;
    private final int mappingIndex;

    private final IncomingRequestListener listener;

    public Mapping(
            final ValidatedConfiguration vc,
            final String mappingName,
            final Optional<LookupService> geoipLookupService,
            final SchemaRegistry schemaRegistry,
            final IncomingRequestListener listener) {
        this.listener = listener;

        final MappingConfiguration mappingConfiguration = vc.configuration().mappings.get(mappingName);
        final Schema schema = schemaRegistry.getSchemaByMappingName(mappingName);

        this.mappingIndex = vc.configuration().mappingIndex(mappingName);
        this.keepCorrupted = !mappingConfiguration.discardCorrupted;
        this.keepDuplicates = !mappingConfiguration.discardDuplicates;

        this.mapper = mappingConfiguration.mappingScriptFile
            .map((mappingScriptFile) -> {
                logger.info("Using script based schema mapping.");
                return new DslRecordMapper(vc, mappingScriptFile, schema, geoipLookupService);
            }).orElseGet(() -> {
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

    public Optional<Item<AvroRecordBuffer>> map(final Item<UndertowEvent> originalIem, final DivolteEvent parsedEvent, final boolean duplicate) {
        if (
                (keepDuplicates || !duplicate) &&
                (keepCorrupted || !parsedEvent.corruptEvent)) {
            final GenericRecord avroRecord = mapper.newRecordFromExchange(parsedEvent);
            final AvroRecordBuffer avroBuffer = AvroRecordBuffer.fromRecord(
                    parsedEvent.partyId,
                    parsedEvent.sessionId,
                    avroRecord);

            /*
             * We should really think of a way to get rid of this and test the
             * mapping process in isolation of the server.
             * In the many-to-many setup, this call is potentially amplified.
             */
            listener.incomingRequest(parsedEvent, avroBuffer, avroRecord);

            return Optional.of(Item.withCopiedAffinity(mappingIndex, originalIem, avroBuffer));
        } else {
            return Optional.empty();
        }
    }
}
