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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;
import io.divolte.server.processing.ProcessingPool;
import io.undertow.util.AttachmentKey;

@ParametersAreNonnullByDefault
public final class IncomingRequestProcessor implements ItemProcessor<UndertowEvent> {
    private static final Logger logger = LoggerFactory.getLogger(IncomingRequestProcessor.class);

    public static final AttachmentKey<Boolean> DUPLICATE_EVENT_KEY = AttachmentKey.create(Boolean.class);

    private final ShortTermDuplicateMemory memory;

    // Given a source index, which mappings do we need to apply.
    private final ImmutableList<ImmutableList<Mapping>> mappingsBySourceIndex;
    // Given a mapping index, which sinks do we need to send it to.
    private final ImmutableList<ImmutableList<ProcessingPool<?, AvroRecordBuffer>>> sinksByMappingIndex;

    public IncomingRequestProcessor(final ValidatedConfiguration vc,
                                    final ImmutableMap<String, ProcessingPool<?, AvroRecordBuffer>> sinksByName,
                                    final Optional<LookupService> geoipLookupService,
                                    final SchemaRegistry schemaRegistry,
                                    final IncomingRequestListener listener) {

        memory = new ShortTermDuplicateMemory(vc.configuration().global.mapper.duplicateMemorySize);

        /*
         * Create all Mapping instances based on their config.
         */
        final Map<String, Mapping> mappingsByName = vc.configuration()
          .mappings
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey,
                                    kv -> new Mapping(vc,
                                                      kv.getKey(),
                                                      geoipLookupService,
                                                      schemaRegistry,
                                                      listener)));

        /*
         * Create a mapping from source index to a list of Mapping's that apply
         * to events generated from that source index. Finally, we use a
         * ImmutableList<ImmutableList<Mapping>> as result, not a
         * Map<Integer, ImmutableList<Mapping>> because that way the backing
         * data structure is effectively a two-dimensional array and no hashing
         * is required for retrieval (list indexes are ints already).
         */
        final ArrayList<ImmutableList<Mapping>> sourceMappingResult =             // temporary mutable container for the result
                IntStream.range(0, vc.configuration().sources.size())
                         .<ImmutableList<Mapping>>mapToObj(ignored -> ImmutableList.of())     // initialized with empty lists per default
                         .collect(Collectors.toCollection(ArrayList::new));

        vc.configuration()
          .mappings
          .entrySet()
          .stream()                                                               // stream of entries (mapping_name, mapping_configuration)
          .flatMap(kv -> kv.getValue()
                           .sources
                           .stream()
                           .map(s -> Maps.immutableEntry(vc.configuration().sourceIndex(s),
                                                         kv.getKey())))           // Results in stream of (source_index, mapping_name)
          .collect(Collectors.groupingBy(Map.Entry::getKey,
                                         Collectors.mapping(e -> mappingsByName.get(e.getValue()),
                                                            MoreCollectors.toImmutableList())
                  ))                                                              // Results in a Map<Integer, ImmutableList<Mapping>> where the key is the source index
          .forEach(sourceMappingResult::set);                                     // Populate the temporary result in ArrayList<ImmutableList<Mapping>>

        mappingsBySourceIndex = ImmutableList.copyOf(sourceMappingResult);        // Make immutable copy

        /*
         * Create a mapping from mapping index to a list of sinks (ProcessingPools)
         * that apply for events that came from the given mapping. Similar as above,
         * we transform the result into a list of lists, instead of a map in order
         * to make sure the underlying lookups are array index lookups instead of
         * hash map lookups.
         *
         * Note that we need to know the sinks for a mapping here, instead of on the
         * sink thread side, since we have one pool per sink at this moment. Later
         * we'll likely move to one pool per sink type (i.e. Kafka, HDFS) and leave
         * it to that pool to multiplex events to different sinks destinations (HDFS
         * files or Kafka topics), which should move this code elsewhere.
         */
        final ArrayList<ImmutableList<ProcessingPool<?,AvroRecordBuffer>>> mappingMappingResult =                          // temporary mutable container for the result
                IntStream.range(0, vc.configuration().mappings.size())
                         .<ImmutableList<ProcessingPool<?,AvroRecordBuffer>>>mapToObj(ignored -> ImmutableList.of())       // initialized with empty lists per default
                         .collect(Collectors.toCollection(ArrayList::new));

        /*
         * Without the intermediate variable (collected), The Eclipse compiler's type
         * inference doesn't know how to handle this. Don't know about Oracle Java compiler.
         */
        final Map<Integer, ImmutableList<ProcessingPool<?, AvroRecordBuffer>>> collected = vc.configuration()
                .mappings
                .entrySet()
                .stream()
                .flatMap(kv->kv.getValue()
                               .sinks
                               .stream()
                               .map(s -> Maps.immutableEntry(vc.configuration().mappingIndex(kv.getKey()), s)))
                .filter(e -> sinksByName.containsKey(e.getValue()))
          .collect(Collectors.groupingBy(Map.Entry::getKey,
                                         Collectors.mapping(e -> sinksByName.get(e.getValue()),
                                                            MoreCollectors.toImmutableList())));
          collected.forEach(mappingMappingResult::set);

          sinksByMappingIndex = ImmutableList.copyOf(mappingMappingResult);
    }

    @Override
    public ProcessingDirective process(final Item<UndertowEvent> item) {
        final DivolteEvent event;
        try {
            event = item.payload.parseRequest();
        } catch (final IncompleteRequestException e) {
            logger.warn("Improper request received from {}.", Optional.ofNullable(item.payload.exchange.getSourceAddress()).map(InetSocketAddress::getHostString).orElse("<UNKNOWN HOST>"));
            return CONTINUE;
        }

        final boolean duplicate = memory.isProbableDuplicate(event.partyId.value, event.sessionId.value, event.eventId);
        event.exchange.putAttachment(DUPLICATE_EVENT_KEY, duplicate);

        mappingsBySourceIndex.get(item.sourceId)
                             .stream()                                                          // For each mapping that applies to this source
                             .map(mapping -> mapping.map(item, event, duplicate))
                             .filter(Optional::isPresent)                                       // Filter discarded for duplication or corruption
                             .map(Optional::get)
                             .forEach(bufferItem -> sinksByMappingIndex.get(bufferItem.sourceId)
                                                                       .stream()                // For each sink that applies to this mapping
                                                                       .forEach(sink -> sink.enqueue(bufferItem)));
        return CONTINUE;
    }
}
