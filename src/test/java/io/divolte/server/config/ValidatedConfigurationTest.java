package io.divolte.server.config;

import static org.junit.Assert.*;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
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
        final Config config =
                ConfigFactory.parseString(
                                "divolte.javascript.name = 404.exe\n")
                                .withFallback(ConfigFactory.parseResources("reference-test.conf"));

        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);
        assertFalse(vc.errors().isEmpty());
        assertEquals("Property 'divolte.javascript.name' must match \"^[A-Za-z0-9_-]+\\.js$\". Found: '404.exe'.", vc.errors().get(0));
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
        assertEquals(ConfigException.Parse.class, vc.errors().get(0).getClass());
    }

    @Test
    public void shouldMapReferenceConfig() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
        assertTrue(vc.errors().isEmpty());
    }
}
