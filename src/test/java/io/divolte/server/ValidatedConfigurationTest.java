package io.divolte.server;

import static org.junit.Assert.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.jasonclawson.jackson.dataformat.hocon.HoconTreeTraversingParser;
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

	@Test
	public void shouldMapReferenceConfig() {
	    final ValidatedConfiguration vc = new ValidatedConfiguration(ConfigFactory::load);
	    assertTrue(vc.errors().isEmpty());
	}
	
	@Test
	public void shouldMapPolymorphicTypes() throws JsonParseException, JsonMappingException, IOException {
        final Config resolved = ConfigFactory.load("poly-test.conf").getConfig("root");
        final ObjectMapper mapper = new ObjectMapper();
        
        // snake_casing
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy());

        // Ignore unknown stuff in the config
//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Deserialization for Duration
        final SimpleModule module= new SimpleModule("Configuration Deserializers");
        module.addDeserializer(Duration.class, new ValidatedConfiguration.DurationDeserializer(resolved));
        module.addDeserializer(Properties.class, new ValidatedConfiguration.PropertiesDeserializer(resolved));

        mapper.registerModules(
                new Jdk8Module(),                   // JDK8 types (Optional, etc.)
                new ParameterNamesModule(),         // Support JDK8 parameter name discovery
                module                              // Register custom deserializers module
                );
        
        TestConfig result = mapper.readValue(new HoconTreeTraversingParser(resolved.root()), TestConfig.class);
        System.out.println(result);
	}
	
	public static class TestConfig {
	    public final String foo;
	    public final Duration duration;
	    public final PolyBase poly;
	    public final Properties props;
	    
	    @JsonCreator
        public TestConfig(final String foo, final Duration duration, final PolyBase poly, final Properties props) {
            this.foo = foo;
            this.duration = duration;
            this.poly = poly;
            this.props = props;
        }

        @Override
        public String toString() {
            return "TestConfig [foo=" + foo + ", duration=" + duration + ", poly=" + poly + ", props=" + props + "]";
        }
	}
	
    @JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
        @Type(value=PolyOne.class, name = "one"),
        @Type(value=PolyTwo.class, name = "two")
    })
	public static abstract class PolyBase {
	    public final String type;
	    public final Duration polyDuration;
	    public final Integer number;
	    
	    @JsonCreator
        public PolyBase(final String type, final Duration polyDuration, final Integer number) {
            this.type = type;
            this.polyDuration = polyDuration;
            this.number = number;
        }

        @Override
        public String toString() {
            return "PolyBase [type=" + type + ", duration=" + polyDuration + ", number=" + number + "]";
        }
	}
	
	public static class PolyOne extends PolyBase {
	    public final String one;

	    @JsonCreator
        public PolyOne(final String type, final Duration duration, final Integer number, final String one) {
            super(type, duration, number);
            this.one = one;
        }

        @Override
        public String toString() {
            return "PolyOne [one=" + one + ", type=" + type + ", duration=" + polyDuration + ", number=" + number + "]";
        }
	}
	
    public static class PolyTwo extends PolyBase {
        public final Duration two;

        @JsonCreator
        public PolyTwo(final String type, final Duration duration, final Integer number, final Duration two) {
            super(type, duration, number);
            this.two = two;
        }

        @Override
        public String toString() {
            return "PolyTwo [two=" + two + ", type=" + type + ", duration=" + polyDuration + ", number=" + number + "]";
        }
    }
}
