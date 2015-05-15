/*
 * Copyright 2015 GoDataDriven B.V.
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

package io.divolte.server.mincode;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Random;

import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
public class MincodeParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new MincodeFactory());

    private static JsonParser createParser(final String s) throws IOException {
        return MAPPER.getFactory().createParser(s);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBinaryDecoding() throws IOException {
        // Fixture.
        final byte[] binaryData = Resources.toByteArray(Resources.getResource("transparent1x1.gif"));
        final String mincode = 's' + Base64.getEncoder().encodeToString(binaryData) + '!';

        // Test itself.
        assertArrayEquals(binaryData, MAPPER.readValue(mincode, byte[].class));
    }

    // Size of the internal buffer that Jackson parsers use.
    private static final int JACKSON_BUFFER_SIZE = 4000;

    @Test
    public void testLargeStringMincodeDecoding() throws IOException {
        // Build a fixture by repeating a record multiple times.
        // (The template will be the same length as records.)
        final String propertyNameTemplate = "property%04X";
        final String propertyValueTemplate = "still more %04X values";
        final String recordTemplate = 's' + propertyNameTemplate + '!' + propertyValueTemplate + '!';
        // Check that the record length and buffer size are relatively prime; this
        // ensures the record will be read at all possible offsets relative to the
        // internal buffer.
        assertEquals("Text size should be relatively prime to the buffer size.",
                     BigInteger.ONE,
                     BigInteger.valueOf(recordTemplate.length()).gcd(BigInteger.valueOf(JACKSON_BUFFER_SIZE)));
        final StringBuilder sb = new StringBuilder(2 + JACKSON_BUFFER_SIZE * recordTemplate.length());
        sb.append('(');
        for (int i = 0; i < JACKSON_BUFFER_SIZE; ++i) {
            sb.append(String.format(recordTemplate, i, i));
        }
        sb.append(')');

        // Time for the test itself.
        final JsonParser parser = createParser(sb.toString());

        // Before starting, everything should be clear.
        assertNull(parser.getCurrentToken());
        assertJsonParserTextUnavailable(parser);

        // First token is an object.
        assertEquals(JsonToken.START_OBJECT, parser.nextToken());
        assertJsonParserText(parser, "{");

        // Now we're going to loop over the strings.
        for (int i = 0; i < JACKSON_BUFFER_SIZE; ++i) {
            final String expectedPropertyName = String.format(propertyNameTemplate, i);
            final String expectedPropertyValue = String.format(propertyValueTemplate, i);

            // First we test the property name.
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals(expectedPropertyName, parser.getCurrentName());
            // Field names are always available via text.
            assertJsonParserText(parser, expectedPropertyName);

            // Next test the property value.
            assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
            assertEquals(expectedPropertyValue, parser.getValueAsString());
            assertJsonParserText(parser, expectedPropertyValue);
            assertTrue(parser.hasTextCharacters());
            // While looking at the value, the name is still available.
            assertEquals(expectedPropertyName, parser.getCurrentName());
        }

        // Next we expect the end of the array.
        assertEquals(JsonToken.END_OBJECT, parser.nextToken());
        assertJsonParserText(parser, "}");

        // Reading beyond the end yields no more tokens...
        assertNull(parser.nextToken());
        assertJsonParserTextUnavailable(parser);

        // ... even if we keep trying.
        assertNull(parser.nextToken());
        assertJsonParserTextUnavailable(parser);
    }

    private static void assertJsonParserText(final JsonParser parser,
                                             final String expectedText) throws IOException {
        assertEquals(expectedText, parser.getText());
        final char[] buffer = parser.getTextCharacters();
        assertNotNull(buffer);
        assertEquals(expectedText, new String(buffer, parser.getTextOffset(), parser.getTextLength()));
    }

    private static void assertJsonParserTextUnavailable(JsonParser parser) throws IOException {
        assertNull(parser.getText());
        assertNull(parser.getTextCharacters());
        assertEquals(0, parser.getTextOffset());
        assertEquals(0, parser.getTextLength());
    }

    // None of these need to be escaped.
    private static char[] SAFE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private static String createLargeStringValue() {
        final int length = 3 * JACKSON_BUFFER_SIZE + 1;
        final StringBuilder sb = new StringBuilder(length);
        final Random random = new Random(0);
        for (int i = 0; i < length; ++i) {
            sb.append(SAFE_CHARS[random.nextInt(SAFE_CHARS.length)]);
        }
        return sb.toString();
    }

    @Test
    public void testLongLargeStringValueDecoding() throws IOException {
        // Build a very large string value to decode.
        final String value = createLargeStringValue();
        final String record = 's' + value + '!';

        // Using a reader ensures the parser performs its own buffering instead
        // of just scanning directly from the string.
        assertEquals(value, MAPPER.readValue(new StringReader(record), String.class));
    }

    @Test
    public void testFirstRecordCannotBeEndOfObject() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Unexpected end of object");
        createParser(")").nextToken();
    }

    @Test
    public void testFirstRecordCannotBeEndOfArray() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Unexpected end of array");
        createParser(".").nextToken();
    }

    @Test
    public void testInvalidRecordType() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Unknown record type");
        createParser("z").nextToken();
    }

    @Test
    public void testUnterminatedStringRecord() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Unexpected end-of-input: was expecting end of string value");
        createParser("sThis record isn't terminated").nextToken();
    }

    @Test
    public void testUnterminatedEscapeSequenceInStringRecord() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Unexpected end-of-input in character escape sequence");
        createParser("sThis record ends with an incomplete escape sequence: ~").nextToken();
    }

    @Test
    public void testInvalidIntegerRecord() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Invalid integer record");
        expectedException.expectCause(Matchers.isA(NumberFormatException.class));
        createParser("d54@@!").nextToken();
    }

    @Test
    public void testInvalidFloatingPointRecord() throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage("Invalid number record");
        expectedException.expectCause(Matchers.isA(NumberFormatException.class));
        createParser("j54@@!").nextToken();
    }

    @Test
    public void testFloatingPointIntegerRecord() throws IOException {
        // Floating point records can also supply integers. In this case
        // we should supply the correct token type.
        final JsonParser parser = createParser("j1234!");
        assertEquals(JsonToken.VALUE_NUMBER_INT, parser.nextToken());
        assertEquals(1234, parser.getValueAsInt());
    }

    // Per JACKSON-804 the max for a byte is 255.
    private static final BigInteger MAX_BYTE = BigInteger.valueOf(255);
    private static final BigInteger MAX_SHORT = BigInteger.valueOf(Short.MAX_VALUE);
    private static final BigInteger MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);

    private static <T extends Number> void assertInRange(final BigInteger value,
                                                         final Class<T> type) throws IOException {
        final T parsedValue = MAPPER.readValue("j" + value + '!', type);
        assertEquals(value, MAPPER.convertValue(parsedValue, BigInteger.class));
    }

    private static <T extends Number> void assertInRange(final BigInteger expectedValue,
                                                         final BigInteger value,
                                                         final Class<T> type) throws IOException {
        final T parsedValue = MAPPER.readValue("j" + value + '!', type);
        assertEquals(expectedValue, MAPPER.convertValue(parsedValue.longValue(), BigInteger.class));
    }

    private <T extends Number> void assertOutOfRange(final String expectedMessageSubstring,
                                                     final BigInteger maxValue,
                                                     final Class<T> type) throws IOException {
        expectedException.expect(JsonParseException.class);
        expectedException.expectMessage(expectedMessageSubstring);
        MAPPER.readValue("j" + maxValue.add(BigInteger.ONE) + '!', type);
    }

    @Test
    public void testIntegerValueByteInRange() throws IOException {
        assertInRange(BigInteger.valueOf(-1), MAX_BYTE, Byte.class);
    }

    @Test
    public void testIntegerValueByteOutOfRange() throws IOException {
        assertOutOfRange("out of range of Java byte", MAX_BYTE, Byte.class);
    }

    @Test
    public void testIntegerValueShortInRange() throws IOException {
        assertInRange(MAX_SHORT, Short.class);
    }

    @Test
    public void testIntegerValueShortOutOfRange() throws IOException {
        assertOutOfRange("out of range of Java short", MAX_SHORT, Short.class);
    }

    @Test
    public void testIntegerValueIntegerInRange() throws IOException {
        assertInRange(MAX_INT, Integer.class);
    }

    @Test
    public void testIntegerValueIntegerOutOfRange() throws IOException {
        assertOutOfRange("out of range of int", MAX_INT, Integer.class);
    }

    @Test
    public void testIntegerValueLongInRange() throws IOException {
        assertInRange(MAX_LONG, Long.class);
    }

    @Test
    public void testIntegerValueLongOutOfRange() throws IOException {
        assertOutOfRange("out of range of long", MAX_LONG, Long.class);
    }

    @Test
    public void testIntegerValueBigIntegerInRange() throws IOException {
        assertInRange(MAX_BYTE,                      BigInteger.class);
        assertInRange(MAX_BYTE.add(BigInteger.ONE),  BigInteger.class);
        assertInRange(MAX_SHORT,                     BigInteger.class);
        assertInRange(MAX_SHORT.add(BigInteger.ONE), BigInteger.class);
        assertInRange(MAX_INT,                       BigInteger.class);
        assertInRange(MAX_INT.add(BigInteger.ONE),   BigInteger.class);
        assertInRange(MAX_LONG,                      BigInteger.class);
        assertInRange(MAX_LONG.add(BigInteger.ONE),  BigInteger.class);
    }

    @Test
    public void testOnlyReadWhatIsRequired() throws IOException {
        final Reader reader = new StringReader("sA string record!Extra trailing data.");
        final JsonParser parser = MAPPER.getFactory().createParser(reader);
        assertEquals("A string record", MAPPER.readValue(parser, String.class));

        // Check the trailing data can be retrieved.
        final StringWriter writer = new StringWriter();
        parser.releaseBuffered(writer);
        assertTrue(0 < writer.getBuffer().length());
        CharStreams.copy(reader, writer);

        // Check the trailer could be retrieved.
        assertEquals("Extra trailing data.", writer.toString());
    }
}
