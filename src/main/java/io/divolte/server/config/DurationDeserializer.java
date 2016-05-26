package io.divolte.server.config;

import static com.fasterxml.jackson.core.JsonToken.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.typesafe.config.impl.ConfigImplUtil;

@ParametersAreNonnullByDefault
public class DurationDeserializer extends StdScalarDeserializer<Duration> {
    private static final long serialVersionUID = 1L;

    public DurationDeserializer() {
        super(Duration.class);
    }

    @Override
    public Duration deserialize(final JsonParser p,
                                final DeserializationContext ctx) throws IOException {
        if (VALUE_STRING != p.getCurrentToken()) {
            throw ctx.mappingException("Expected string value for Duration mapping.");
        }
        long result;
        try {
            result = parse(p.getText());
        } catch(final DurationFormatException e) {
            throw new JsonMappingException(p, e.getMessage(), e);
        }
        return Duration.ofNanos(result);
    }

    public static Duration parseDuration(final String input) {
        return Duration.ofNanos(parse(input));
    }

    // Inspired by Typesafe Config parseDuration(...)
    private static long parse(final String input) {
        final String s = ConfigImplUtil.unicodeTrim(input);
        final String originalUnitString = getUnits(s);
        String unitString = originalUnitString;
        final String numberString = ConfigImplUtil.unicodeTrim(s.substring(0, s.length() - unitString.length()));

        // this would be caught later anyway, but the error message
        // is more helpful if we check it here.
        if (numberString.isEmpty()) {
            final String msg = String.format("No number in duration value '%s'", input);
            throw new DurationFormatException(msg);
        }

        // All units longer than 2 characters are accepted in singular or plural form.
        // This normalizes to plural so we only need to check that below.
        if (unitString.length() > 2 && !unitString.endsWith("s")) {
            unitString += 's';
        }

        // note that this is deliberately case-sensitive
        final TimeUnit units;
        switch (unitString) {
            case "":
            case "ms":
            case "millis":
            case "milliseconds":
                units = TimeUnit.MILLISECONDS;
                break;
            case "us":
            case "micros":
            case "microseconds":
                units = TimeUnit.MICROSECONDS;
                break;
            case "ns":
            case "nanos":
            case "nanoseconds":
                units = TimeUnit.NANOSECONDS;
                break;
            case "d":
            case "days":
                units = TimeUnit.DAYS;
                break;
            case "h":
            case "hours":
                units = TimeUnit.HOURS;
                break;
            case "s":
            case "seconds":
                units = TimeUnit.SECONDS;
                break;
            case "m":
            case "minutes":
                units = TimeUnit.MINUTES;
                break;
            default:
                final String msg = String.format("Could not parse time unit '%s' (try ns, us, ms, s, m, h, d)", originalUnitString);
                throw new DurationFormatException(msg);
        }

        try {
            // if the string is purely digits, parse as an integer to avoid
            // possible precision loss; otherwise as a double.
            return numberString.matches("[0-9]+")
                    ? units.toNanos(Long.parseLong(numberString))
                    : (long) (Double.parseDouble(numberString) * units.toNanos(1));
        } catch (final NumberFormatException e) {
            final String msg = String.format("Could not parse duration number '%s'", numberString);
            throw new DurationFormatException(msg);
        }
    }

    private static String getUnits(final String s) {
        int i = s.length() - 1;
        while (i >= 0) {
            final char c = s.charAt(i);
            if (!Character.isLetter(c)) {
                break;
            }
            i -= 1;
        }
        return s.substring(i + 1);
    }
}
