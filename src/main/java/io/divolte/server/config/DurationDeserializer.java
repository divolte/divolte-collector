package io.divolte.server.config;

import static com.fasterxml.jackson.core.JsonToken.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.typesafe.config.impl.ConfigImplUtil;

public class DurationDeserializer extends StdScalarDeserializer<Duration> {
    private static final long serialVersionUID = 1L;

    public DurationDeserializer() {
        super(Duration.class);
    }

    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
        if (VALUE_STRING == p.getCurrentToken()) {
            return Duration.ofNanos(parseDuration(p.getText(), ctx));
        } else {
            throw ctx.mappingException("Expected string value for Duration mapping.");
        }
    }

    // Inspired by Typesafe Config parseDuration(...)
    private static long parseDuration(final String input, final DeserializationContext context) throws JsonMappingException {
        String s = ConfigImplUtil.unicodeTrim(input);
        String originalUnitString = getUnits(s);
        String unitString = originalUnitString;
        String numberString = ConfigImplUtil.unicodeTrim(s.substring(0, s.length() - unitString.length()));
        TimeUnit units = null;

        // this would be caught later anyway, but the error message
        // is more helpful if we check it here.
        if (numberString.isEmpty()) {
            throw context.mappingException(String.format("No number in duration value '%s'", input));
        }

        if (unitString.length() > 2 && !unitString.endsWith("s")) {
            unitString = unitString + "s";
        }

        // note that this is deliberately case-sensitive
        if (unitString.equals("") || unitString.equals("ms") || unitString.equals("millis") || unitString.equals("milliseconds")) {
            units = TimeUnit.MILLISECONDS;
        } else if (unitString.equals("us") || unitString.equals("micros") || unitString.equals("microseconds")) {
            units = TimeUnit.MICROSECONDS;
        } else if (unitString.equals("ns") || unitString.equals("nanos") || unitString.equals("nanoseconds")) {
            units = TimeUnit.NANOSECONDS;
        } else if (unitString.equals("d") || unitString.equals("days")) {
            units = TimeUnit.DAYS;
        } else if (unitString.equals("h") || unitString.equals("hours")) {
            units = TimeUnit.HOURS;
        } else if (unitString.equals("s") || unitString.equals("seconds")) {
            units = TimeUnit.SECONDS;
        } else if (unitString.equals("m") || unitString.equals("minutes")) {
            units = TimeUnit.MINUTES;
        } else {
            throw context.mappingException(String.format("Could not parse time unit '%s' (try ns, us, ms, s, m, h, d)", originalUnitString));
        }

        try {
            // if the string is purely digits, parse as an integer to avoid
            // possible precision loss;
            // otherwise as a double.
            if (numberString.matches("[0-9]+")) {
                return units.toNanos(Long.parseLong(numberString));
            } else {
                long nanosInUnit = units.toNanos(1);
                return (long) (Double.parseDouble(numberString) * nanosInUnit);
            }
        } catch (NumberFormatException e) {
            throw context.mappingException(String.format("Could not parse duration number '%s'", numberString));
        }
    }

    private static String getUnits(String s) {
        int i = s.length() - 1;
        while (i >= 0) {
            char c = s.charAt(i);
            if (!Character.isLetter(c)) {
                break;
            }
            i -= 1;
        }
        return s.substring(i + 1);
    }
}
