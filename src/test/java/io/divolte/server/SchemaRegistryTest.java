package io.divolte.server;

import com.typesafe.config.ConfigFactory;
import io.divolte.server.config.ValidatedConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SchemaRegistryTest {

    @Test
    public void testConfluentSinkReceivesSchemaId() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("schema-registry-with-confluent.conf"));
        final SchemaRegistry registry = new SchemaRegistry(vc);
        DivolteSchema kafka = registry.getSchemaBySinkName("kafka");
        assertTrue(kafka.confluentId.isPresent());
    }
}
