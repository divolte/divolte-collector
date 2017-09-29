/*
 * Copyright 2017 GoDataDriven B.V.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.divolte.record.DefaultEventRecord;
import io.divolte.server.config.MappingConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class SchemaRegistry {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistry.class);

    private final ImmutableMap<String,Schema> schemasByMappingName;
    private final ImmutableMap<String,Schema> schemasBySinkName;

    public SchemaRegistry(final ValidatedConfiguration vc) {
        final ImmutableMap<String, MappingConfiguration> mappings = vc.configuration().mappings;

        // Build a mapping of the schema location for each mapping.
        final ImmutableMap<String,Optional<String>> schemaLocationsByMapping =
                ImmutableMap.copyOf(Maps.transformValues(mappings, config -> config.schemaFile));

        // Load the actual schemas. Once.
        logger.debug("Loading schemas for mappings: {}", schemaLocationsByMapping.keySet());
        final ImmutableMap<Optional<String>,Schema> schemasByLocation =
                schemaLocationsByMapping.values()
                                        .stream()
                                        .distinct()
                                        .collect(ImmutableMap.toImmutableMap(Function.identity(), SchemaRegistry::loadSchema));

        // Store the schema for each mapping.
        schemasByMappingName =
                ImmutableMap.copyOf(Maps.transformValues(schemaLocationsByMapping, schemasByLocation::get));
        logger.info("Loaded schemas used for mappings: {}", schemasByMappingName.keySet());

        // Also calculate an inverse mapping by sink name.
        // (Validation will ensure that multiple mappings for each sink have the same value.)
        schemasBySinkName =
                mappings.values()
                        .stream()
                        .flatMap(config -> config.sinks
                                                 .stream()
                                                 .map(sink ->
                                                         Maps.immutableEntry(sink,
                                                                             schemasByLocation.get(config.schemaFile))))
                        .distinct()
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        logger.info("Inferred schemas used for sinks: {}", schemasBySinkName.keySet());
    }

    public Schema getSchemaByMappingName(final String mappingName) {
        final Schema schema = schemasByMappingName.get(mappingName);
        Preconditions.checkArgument(null != schema, "Illegal mapping name: %s", mappingName);
        return schema;
    }

    public Schema getSchemaBySinkName(final String sinkName) {
        final Schema schema = schemasBySinkName.get(sinkName);
        // This means that the sink either doesn't exist, or isn't associated with a mapping.
        // (Without a mapping, we can't infer the schema.)
        Preconditions.checkArgument(null != schema, "Illegal sink name: %s", sinkName);
        return schema;
    }

    private static Schema loadSchema(final Optional<String> schemaLocation) {
        return schemaLocation
                .map(filename -> {
                    final Schema.Parser parser = new Schema.Parser();
                    logger.info("Loading Avro schema from path: {}", filename);
                    try {
                        return parser.parse(new File(filename));
                    } catch(final IOException ioe) {
                        logger.error("Failed to load Avro schema file.");
                        throw new UncheckedIOException("Failed to load Avro schema file.", ioe);
                    }
                })
                .orElseGet(() -> {
                    logger.info("Using builtin default Avro schema.");
                    return DefaultEventRecord.getClassSchema();
                });
    }
}
