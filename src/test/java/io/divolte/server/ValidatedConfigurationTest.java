package io.divolte.server;

import static org.junit.Assert.*;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

public class ValidatedConfigurationTest {
    @Test
    public void shouldNotThrowExceptionsOnInvalidConfiguration() {
        final Config empty = ConfigFactory.parseString("");
        ValidatedConfiguration vc = new ValidatedConfiguration(() -> empty);

        assertFalse(vc.isValid());
        assertFalse(vc.errors().isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowAccessToInvalidConfiguration() {
        final Config empty = ConfigFactory.parseString("");
        ValidatedConfiguration vc = new ValidatedConfiguration(() -> empty);

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
}
