/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.recordmapping;

import io.divolte.server.ServerTestUtils;
import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BytesValueProducerTest {
    private ByteBuffer generateBuffer(final int size) {
        return generateBuffer(0, size, 0);
    }

    private ByteBuffer generateBuffer(final int padBefore, final int size, final int padAfter) {
        final byte[] bytes = new byte[padBefore + size + padAfter];
        for (int i = 0; i < size; ++i) {
            bytes[i + padBefore] = (byte)i;
        }
        return ByteBuffer.wrap(bytes, padBefore, size);
    }

    private static <T> Optional<T> produce(final ValueProducer<T> producer) {
        return producer.produce(ServerTestUtils.createMockBrowserEvent(), new HashMap<>());
    }

    private static BytesValueProducer bytesProducer(final ByteBuffer buffer) {
        return new BytesValueProducer("stubproducer", (e, c) -> Optional.of(buffer));
    }

    @Test
    public void shouldSupportLowercaseHex() {
        final String encodedValue = produce(bytesProducer(generateBuffer(20)).toHexLower()).orElseThrow(AssertionError::new);
        assertEquals("000102030405060708090a0b0c0d0e0f10111213", encodedValue);
    }

    @Test
    public void shouldSupportUppercaseHex() {
        final String encodedValue = produce(bytesProducer(generateBuffer(20)).toHexUpper()).orElseThrow(AssertionError::new);
        assertEquals("000102030405060708090A0B0C0D0E0F10111213", encodedValue);
    }

    @Test
    public void shouldSupportBase64() {
        final String encodedValue = produce(bytesProducer(generateBuffer(70)).toBase64()).orElseThrow(AssertionError::new);
        assertEquals("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERQ==", encodedValue);
    }

    @Test
    public void shouldNotWrapBase64() {
        final String encodedValue = produce(bytesProducer(generateBuffer(100)).toBase64()).orElseThrow(AssertionError::new);
        assertFalse(encodedValue.contains("\n"));
    }

    @Test
    public void shouldSupportSlicedBuffer() {
        // This test ensures that we're only encoding the part of the byte buffer that is valid data.
        final ByteBuffer paddedBytes = generateBuffer(3, 20, 12);
        final String encodedPaddedBytes = produce(bytesProducer(paddedBytes).toHexLower()).orElseThrow(AssertionError::new);
        final String encodedSlicedBytes = produce(bytesProducer(paddedBytes.slice()).toHexLower()).orElseThrow(AssertionError::new);
        assertEquals("000102030405060708090a0b0c0d0e0f10111213", encodedPaddedBytes);
        assertEquals("000102030405060708090a0b0c0d0e0f10111213", encodedSlicedBytes);
    }
}
