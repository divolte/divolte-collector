/*
 * Copyright 2017 GoDataDriven B.V.
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

package io.divolte.server.js;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import com.google.common.io.ByteStreams;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class GzipTest {

    @Test
    public void testGzippedDataCanBeDecompressed() throws IOException {
        final String sampleData = "This is some text that should compress fairly well,\n" +
                                  "and be long enough that it is worth it given the container overhead.";
        final byte[] input = sampleData.getBytes(StandardCharsets.UTF_8);

        // Check that it compresses.
        final Optional<ByteBuffer> gzippedInput = Gzip.compress(input);
        assertThat(gzippedInput.isPresent(), is(true));
        final ByteBuffer gzippedBuffer = gzippedInput.get();
        assertThat(gzippedBuffer.remaining(), is(lessThan(input.length)));

        // Verify that uncompressing it yields the original.
        final byte[] gunzipped = gunzip(gzippedBuffer);
        assertThat(gunzipped, is(equalTo(input)));
    }

    @Test
    public void testSmallInputNotCompressed() throws IOException {
        final String smallSample = "Too small";
        final byte[] input = smallSample.getBytes(StandardCharsets.UTF_8);

        // Check that it doesn't compress.
        final Optional<ByteBuffer> gzippedInput = Gzip.compress(input);
        assertThat(gzippedInput.isPresent(), is(false));
    }

    @Test
    public void testUncompressableNotCompressed() throws NoSuchAlgorithmException {
        final byte[] input = new byte[1024];
        SecureRandom.getInstance("SHA1PRNG").nextBytes(input);

        // Check that it doesn't compress.
        final Optional<ByteBuffer> gzippedInput = Gzip.compress(input);
        assertThat(gzippedInput.isPresent(), is(false));
    }

    private static byte[] gunzip(final ByteBuffer input) throws IOException {
        try (final ByteArrayInputStream bis = new ByteArrayInputStream(input.array(), input.arrayOffset(), input.remaining());
             final GZIPInputStream is = new GZIPInputStream(bis)) {
             return ByteStreams.toByteArray(is);
        }
    }
}
