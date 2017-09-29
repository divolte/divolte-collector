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
