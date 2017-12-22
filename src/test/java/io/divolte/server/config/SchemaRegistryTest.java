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

package io.divolte.server.config;

import com.typesafe.config.ConfigFactory;
import io.divolte.server.DivolteSchema;
import io.divolte.server.SchemaRegistry;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class SchemaRegistryTest {

    @Test
    public void testSinkCorrectlyAssociatedWithSchema() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("schema-registry-with-confluent.conf"));
        final SchemaRegistry registry = new SchemaRegistry(vc);
        // This throws an exception if the sink is unknown.
        registry.getSchemaBySinkName("kafka");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSchemaForUnknownSinkThrowsException() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("reference.conf"));
        final SchemaRegistry registry = new SchemaRegistry(vc);
        // This throws an exception if the sink is unknown.
        registry.getSchemaBySinkName("an-unknown-sink");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSchemaForUnknownMappingThrowsException() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("reference.conf"));
        final SchemaRegistry registry = new SchemaRegistry(vc);
        // This throws an exception if the mapping is unknown.
        registry.getSchemaByMappingName("an-unknown-mapping");
    }

    @Test
    public void testConfluentSinkCorrectlyAssociatedWithSchemaId() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("schema-registry-with-confluent.conf"));
        final SchemaRegistry registry = new SchemaRegistry(vc);
        final DivolteSchema schema = registry.getSchemaBySinkName("kafka");
        assertEquals(schema.confluentId, Optional.of(12345));
    }
}
