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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.format.DataFormatDetector;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
public class MincodeFactoryTest {

    private static final String MINCODE = "sAn arbitrary string!";
    private static final String EXPECTED_STRING = "An arbitrary string";

    private ObjectMapper mincodeMapper;
    private Optional<Path> cleanupFile = Optional.empty();

    @Before
    public void setUp() throws Exception {
        mincodeMapper = new ObjectMapper(new MincodeFactory());
    }

    @After
    public void tearDown() throws Exception {
        mincodeMapper = null;
        cleanupFile.ifPresent(path -> {
            try {
                Files.deleteIfExists(path);
            } catch (final IOException e) {
                throw new RuntimeException("Error cleaning up temporary file", e);
            }
            cleanupFile = Optional.empty();
        });
    }

    private Path createTemporyFile(final String content) throws IOException {
        final Path tempFile = Files.createTempFile("mincodetest", "mc");
        cleanupFile = Optional.of(tempFile);
        Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));
        return tempFile;
    }

    @Test
    public void testReadingFromString() throws Exception {
        assertEquals(EXPECTED_STRING,
                mincodeMapper.readValue(MINCODE, String.class));
    }

    @Test
    public void testReadingFromByteArray() throws Exception {
        final byte[] bytes = MINCODE.getBytes(StandardCharsets.UTF_8);
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(bytes, String.class));
        mincodeMapper.getFactory().setInputDecorator(INVERTING_DECORATOR);
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(invert(bytes), String.class));
    }

    @Test
    public void testReadingFromPartOfByteArray() throws Exception {
        // Create an array with our bytes to decode in the middle somewhere.
        final byte[] needle = MINCODE.getBytes(StandardCharsets.UTF_8);
        final byte[] haystack = new byte[needle.length + 780];
        System.arraycopy(needle, 0, haystack, 45, needle.length);

        // The actual test.
        assertEquals(EXPECTED_STRING, mincodeMapper.readValue(haystack, 45, needle.length, String.class));
    }

    @Test
    public void testReadingFromReader() throws Exception {
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(new StringReader(MINCODE), String.class));
    }

    @Test
    public void testReadingFromFile() throws Exception {
        final Path file = createTemporyFile(MINCODE);
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(file.toFile(), String.class));
    }

    @Test
    public void testReadingFromUrl() throws Exception {
        final Path file = createTemporyFile(MINCODE);
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(file.toUri().toURL(), String.class));
    }

    @Test
    public void testReadingFromInputStream() throws Exception {
        final Path file = createTemporyFile(MINCODE);
        try (final InputStream is = Files.newInputStream(file)) {
            assertEquals(EXPECTED_STRING,
                         mincodeMapper.readValue(is, String.class));
        }
    }

    @Test
    public void testReadingFromCharArray() throws Exception {
        final char[] chars = MINCODE.toCharArray();
        final JsonFactory factory = mincodeMapper.getFactory();
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(factory.createParser(chars), String.class));
        factory.setInputDecorator(INVERTING_DECORATOR);
        assertEquals(EXPECTED_STRING,
                     mincodeMapper.readValue(factory.createParser(invert(chars)), String.class));
    }

    @Test
    public void testReadingFromPartOfCharArray() throws Exception {
        // Create an array with our characters to decode in the middle somewhere.
        final char[] needle = MINCODE.toCharArray();
        final char[] haystack = new char[needle.length + 780];
        System.arraycopy(needle, 0, haystack, 45, needle.length);

        final JsonParser parser = mincodeMapper.getFactory().createParser(haystack, 45, needle.length);
        assertEquals(EXPECTED_STRING, mincodeMapper.readValue(parser, String.class));
    }

    @Test
    public void testDataDetector() throws Exception {
        final JsonFactory factory = mincodeMapper.getFactory();
        final DataFormatDetector detector = new DataFormatDetector(factory);
        assertEquals(factory.getFormatName(), detector.findFormat(MINCODE.getBytes(StandardCharsets.UTF_8)).getMatchedFormatName());
        // Zero-length mincode is invalid.
        assertFalse(detector.findFormat(new byte[0]).hasMatch());
        // 'z' is not a valid record type.
        assertFalse(detector.findFormat("zoop".getBytes(StandardCharsets.UTF_8)).hasMatch());
        // '.' is a valid record type, but not as the first record.
        assertFalse(detector.findFormat(".".getBytes(StandardCharsets.UTF_8)).hasMatch());
    }

    private static byte invert(final byte b) {
        return (byte)(b ^ 0xff);
    }

    private static byte[] invert(final byte[] bytes) {
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = invert(bytes[i]);
        }
        return bytes;
    }

    private static char[] invert(final char[] chars, final int offset, final int length) {
        for (int i = offset; i < offset + length; ++i) {
            chars[i] ^= 0xffff;
        }
        return chars;
    }

    private static char[] invert(final char[] chars) {
        return invert(chars, 0, chars.length);
    }

    private static final InputDecorator INVERTING_DECORATOR = new InputDecorator() {
        @Override
        public InputStream decorate(final IOContext ctxt, final InputStream in) throws IOException {
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    final int b = in.read();
                    return b != -1 ? invert((byte)b) : -1;
                }
                @Override
                public void close() throws IOException {
                    in.close();
                    super.close();
                }
            };
        }

        @Override
        public InputStream decorate(final IOContext ctxt,
                                    final byte[] src,
                                    final int offset,
                                    final int length) throws IOException {
            return new ByteArrayInputStream(invert(src.clone()));
        }

        @Override
        public Reader decorate(final IOContext ctxt, final Reader r) throws IOException {
            return new Reader() {
                @Override
                public int read(final char[] cbuf,
                                final int off,
                                final int len) throws IOException {
                    final int count = r.read(cbuf, off, len);
                    invert(cbuf, off, len);
                    return count;
                }

                @Override
                public void close() throws IOException {
                    r.close();
                }
            };
        }
    };
}
