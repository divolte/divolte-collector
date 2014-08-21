package io.divolte.server;

import com.typesafe.config.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class OptionalConfigTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldProcessAbsentPathsWithoutExceptionForAllTypes() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<String> optStr = OptionalConfig.of(config::getString, "some.non.existing.path");
        checkAbsentNess(optStr);

        OptionalConfig<Integer> optInt = OptionalConfig.of(config::getInt, "some.non.existing.path");
        checkAbsentNess(optInt);

        OptionalConfig<Boolean> optBool = OptionalConfig.of(config::getBoolean, "some.non.existing.path");
        checkAbsentNess(optBool);

        OptionalConfig<Object> optObj = OptionalConfig.of(config::getAnyRef, "some.non.existing.path");
        checkAbsentNess(optObj);

        OptionalConfig<Long> optDuration = OptionalConfig.of(config::getDuration, "some.non.existing.path", TimeUnit.DAYS);
        checkAbsentNess(optDuration);

    }

    @Test
    public void shouldProcessPresentPathsForAllTypes() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.existing.str");
        checkPresentNess(cfgStr, String.class);
        assertEquals("Existing", cfgStr.get());

        OptionalConfig<Integer> cfgInt = OptionalConfig.of(config::getInt, "some.existing.int");
        checkPresentNess(cfgInt, Integer.class);
        assertEquals(42, cfgInt.get().intValue());

        OptionalConfig<Boolean> cfgBool = OptionalConfig.of(config::getBoolean, "some.existing.bool");
        checkPresentNess(cfgBool, Boolean.class);
        assertTrue(cfgBool.get());

        OptionalConfig<Object> cfgObjArray = OptionalConfig.of(config::getAnyRef, "some.existing.obj.array");
        checkPresentNess(cfgObjArray, ArrayList.class);
        assertTrue(cfgObjArray.get() instanceof ArrayList);

        OptionalConfig<Object> cfgObjObject = OptionalConfig.of(config::getAnyRef, "some.existing.obj.obj");
        checkPresentNess(cfgObjObject, Map.class);
        assertEquals("val1", ((Map<?,?>) cfgObjObject.get()).get("key1"));
        assertEquals("val2", ((Map<?,?>) cfgObjObject.get()).get("key2"));

        OptionalConfig<Object> cfgObjBool = OptionalConfig.of(config::getAnyRef, "some.existing.obj.bool");
        checkPresentNess(cfgObjBool, Boolean.class);
        assertFalse((Boolean) cfgObjBool.get());

        OptionalConfig<Object> cfgObjNumber = OptionalConfig.of(config::getAnyRef, "some.existing.obj.number");
        checkPresentNess(cfgObjNumber, Integer.class);
        assertEquals(42, ((Integer) cfgObjNumber.get()).intValue());

        OptionalConfig<Object> cfgObjString = OptionalConfig.of(config::getAnyRef, "some.existing.obj.string");
        checkPresentNess(cfgObjString, String.class);
        assertEquals("internal", cfgObjString.get());

        OptionalConfig<Object> cfgObjNull = OptionalConfig.of(config::getAnyRef, "some.existing.obj.null");
        exception.expect(NoSuchElementException.class);
        cfgObjNull.get();

        OptionalConfig<ConfigValue> cfgValArray = OptionalConfig.of(config::getValue, "some.existing.obj.array");
        checkPresentNess(cfgValArray, ConfigList.class);
        assertTrue(cfgValArray.get().unwrapped() instanceof ArrayList);

        OptionalConfig<ConfigValue> cfgValObject = OptionalConfig.of(config::getValue, "some.existing.obj.obj");
        checkPresentNess(cfgValObject, ConfigObject.class);
        assertTrue(cfgValObject.get().unwrapped() instanceof Map);
        assertEquals("val1", ((Map<?,?>) cfgValObject.get().unwrapped()).get("key1"));
        assertEquals("val2", ((Map<?,?>) cfgValObject.get().unwrapped()).get("key2"));

        OptionalConfig<ConfigValue> cfgValBool = OptionalConfig.of(config::getValue, "some.existing.obj.bool");
        checkPresentNess(cfgValBool, ConfigValue.class);
        assertFalse((Boolean) cfgValBool.get().unwrapped());

        OptionalConfig<ConfigValue> cfgValNumber = OptionalConfig.of(config::getValue, "some.existing.obj.number");
        checkPresentNess(cfgValNumber, ConfigValue.class);
        assertEquals(42, ((Integer) cfgValNumber.get().unwrapped()).intValue());

        OptionalConfig<ConfigValue> cfgValString = OptionalConfig.of(config::getValue, "some.existing.obj.string");
        checkPresentNess(cfgValString, ConfigValue.class);
        assertEquals("internal", cfgValString.get().unwrapped());

        OptionalConfig<ConfigValue> cfgValNull = OptionalConfig.of(config::getValue, "some.existing.obj.null");
        assertNull(cfgValNull.get());

        OptionalConfig<Long> cfgDuration = OptionalConfig.of(config::getDuration, "some.existing.duration", TimeUnit.MILLISECONDS);
        checkPresentNess(cfgDuration, Long.class);
        assertEquals(86400000L, cfgDuration.get().longValue());

        OptionalConfig<Config> cfgConfig = OptionalConfig.of(config::getConfig, "some.existing.config");
        checkPresentNess(cfgConfig, Config.class);
        OptionalConfig<String> cfgStrInConfig = OptionalConfig.of(cfgConfig.get()::getString, "foo");
        checkPresentNess(cfgStrInConfig, String.class);
        assertEquals("bar", cfgStrInConfig.get());
    }

    @Test
    public void shouldProcessMapFunctionForPresentValue() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.existing.str");
        OptionalConfig<Integer> map = cfgStr.map(value -> {
                    return value.hashCode();
                }
        );
        checkPresentNess(map, Integer.class);
        assertEquals(-2058053205, map.get().intValue());
    }

    @Test
    public void shouldProcessMapFunctionForAbsentValue() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.non.existing.str");
        OptionalConfig<Integer> map = cfgStr.map(value -> {
                    return value.hashCode();
                }
        );
        checkAbsentNess(map);
    }


    @Test
    public void shouldProcessFilterFunctionForPresentValue() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<List<String>> cfgArr = OptionalConfig.of(config::getStringList, "some.existing.obj.array");
        OptionalConfig<List<String>> filtered = cfgArr.filter(v -> v.contains("val1"));
        checkPresentNess(filtered, List.class);
        assertEquals(2, filtered.get().size());
        assertEquals("val1", filtered.get().get(0));
        assertEquals("val2", filtered.get().get(1));

        OptionalConfig<List<String>> filteredNoMatch = cfgArr.filter(v -> v.contains("valueThatIsntThere"));
        checkAbsentNess(filteredNoMatch);
    }

    @Test
    public void shouldProcessFilterFunctionForAbsentValue() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<List<String>> cfgArr = OptionalConfig.of(config::getStringList, "some.non.existing.obj.array");
        OptionalConfig<List<String>> filtered = cfgArr.filter(v -> v.contains('1'));
        checkAbsentNess(filtered);
    }

    @Test
    public void shouldProcessOrElseFunctionForPresentValue() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.existing.str");
        String result = cfgStr.orElse("SomethingElse");
        assertEquals("Existing", result);
    }

    @Test
    public void shouldProcessOrElseFunctionForAbsentValue() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.non.existing.str");
        String result = cfgStr.orElse("theOtherOne");
        assertEquals("theOtherOne", result);
    }

    @Test
    public void shouldProcessOrElseGetFunctionForPresentValue() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.existing.str");
        String result = cfgStr.orElseGet("SomethingElse"::toString);
        assertEquals("Existing", result);
    }

    @Test
    public void shouldProcessOrElseGetFunctionForAbsentValue() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.non.existing.str");
        String result = cfgStr.orElseGet("theOtherOne"::toString);
        assertEquals("theOtherOne", result);
    }

    @Test
    public void shouldProcessOrElseThrowFunctionForPresentValue() {
        Config config = ConfigFactory.load("config-test-optionals-present");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.existing.str");
        String result = cfgStr.orElseThrow(() -> new IllegalStateException("This should not happen"));
        assertEquals("Existing", result);
    }

    @Test
    public void shouldProcessOrElseThrowFunctionForAbsentValue() {
        Config config = ConfigFactory.load("config-test-optionals-absent");

        OptionalConfig<String> cfgStr = OptionalConfig.of(config::getString, "some.non.existing.str");
        exception.expect(IllegalStateException.class);
        cfgStr.orElseThrow(() -> new IllegalStateException("This should happen"));
    }


    private void checkAbsentNess(OptionalConfig optional) {
        assertTrue(optional.isAbsent());
        assertFalse(optional.isPresent());
        assertTrue(optional instanceof OptionalConfig.ConfigAbsent);
    }
    private void checkPresentNess(OptionalConfig optional, Class valueType) {
        assertFalse(optional.isAbsent());
        assertTrue(optional.isPresent());
        assertTrue(optional instanceof OptionalConfig.ConfigPresent);
        assertTrue(valueType.isInstance(optional.get()));
    }
}
