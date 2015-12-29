package io.divolte.server.config;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ValidatedConfigurationTest {
    @Test
    public void shouldNotThrowExceptionsOnInvalidConfiguration() {
        final Config empty = ConfigFactory.parseString("");
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> empty);

        assertFalse(vc.isValid());
        assertFalse(vc.errors().isEmpty());
    }

    @Test
    public void shouldValidateJavaScriptName() {
        final String propertyName = "divolte.sources.browser.javascript.name";
        final String invalidValue = "404.exe";
        final Config config = ConfigFactory.parseMap(ImmutableMap.of(propertyName, invalidValue))
                                           .withFallback(ConfigFactory.parseResources("base-test-server.conf"))
                                           .withFallback(ConfigFactory.parseResources("reference-test.conf"));

        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);
        assertFalse(vc.errors().isEmpty());
        final String reportedPropertyName = propertyName.replace(".sources.browser.", ".sources[browser].");
        assertEquals("Property '" + reportedPropertyName + "' must match \"^[A-Za-z0-9_-]+\\.js$\". Found: '" + invalidValue + "'.",
                     vc.errors().get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowAccessToInvalidConfiguration() {
        final Config empty = ConfigFactory.parseString("");
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> empty);

        assertFalse(vc.isValid());

        // This throws IllegalArgumentException in case of invalid configuration
        vc.configuration();
    }

    @Test
    public void shouldNotBreakOnConfigSyntaxErrorsDuringLoad() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseString("not = //allowed"));
        assertFalse(vc.errors().isEmpty());
        assertEquals("String: 1: Expecting a value but got wrong token: end of file", vc.errors().get(0));
    }

    @Test
    public void shouldMapReferenceConfig() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
        assertTrue(vc.errors().isEmpty());
    }

    @Test
    public void shouldReportMissingSourcesAndSinks() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("missing-sources-sinks.conf"));

        assertFalse(vc.isValid());
        assertEquals(1, vc.errors().size());
        assertTrue(
                vc.errors()
                  .get(0)
                  .startsWith("Property 'divolte.' The following sources and/or sinks were used in a mapping but never defined: [missing-sink, missing-source].."));
    }

    @Test
    public void sourceAndSinkNamesCannotCollide() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("source-sink-collisions.conf"));

        assertFalse(vc.isValid());
        assertEquals(1, vc.errors().size());
        assertTrue(
                vc.errors()
                  .get(0)
                  .startsWith("Property 'divolte.' Source and sink names cannot collide (must be globally unique). The following names were both used as source and as sink: [foo, bar].."));
    }

    @Test
    public void sharedSinksAllowedWithSameSchema() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("multiple-mappings-same-schema-shared-sink.conf"));
        assertTrue(vc.isValid());
    }

    @Test
    public void sharedSinksCannotHaveDifferentSchemas() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("multiple-mappings-different-schema-shared-sink.conf"));

        assertFalse(vc.isValid());
        assertEquals(1, vc.errors().size());
        assertTrue(
                vc.errors()
                  .get(0)
                  .startsWith("Property 'divolte.' Any sink can only use one schema. The following sinks have multiple mappings with different schema's linked to them: [kafka].."));
    }
}
