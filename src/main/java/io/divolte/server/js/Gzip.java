/*
 * Copyright 2014 GoDataDriven B.V.
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Utility class for compressing data.
 *
 * This is necessary because {@link java.util.zip.GZIPOutputStream} doesn't let us
 * control the compression level.
 */
@ParametersAreNonnullByDefault
class Gzip {
    // Magic value indicating start of Gzip header.
    private static final int GZIP_MAGIC = 0x8b1f;
    // Pre-calculated Gzip header.
    private static final byte[] GZIP_HEADER = {
            (byte) GZIP_MAGIC,        // Magic number (short)
            (byte)(GZIP_MAGIC >> 8),  // Magic number (short)
            Deflater.DEFLATED,        // Compression method (CM)
            0,                        // Flags (FLG)
            0,                        // Modification time MTIME (int)
            0,                        // Modification time MTIME (int)
            0,                        // Modification time MTIME (int)
            0,                        // Modification time MTIME (int)
            0,                        // Extra flags (XFLG)
            0                         // Operating system (OS)
    };
    // The size of the Gzip header.
    private static final int GZIP_HEADER_LENGTH = GZIP_HEADER.length;
    // The size of the Gzip footer.
    private static final int GZIP_FOOTER_SIZE = 8;
    // The amount of overhead imposed by the Gzip container.
    private static final int GZIP_OVERHEAD = GZIP_HEADER_LENGTH + GZIP_FOOTER_SIZE;

    /**
     * Compress some data as much as possible using the Gzip algorithm.
     * @param input the data to compress.
     * @return the compressed data (including Gzip container), or empty
     *      if the compressed data wasn't smaller than the input.
     */
    public static Optional<ByteBuffer> compress(final byte[] input) {
        return compress(input, 0, input.length);
    }

    /**
     * Compress some data as much as possible using the Gzip algorithm.
     * @param input the data to compress.
     * @return the compressed data (including Gzip container), or empty
     *      if the compressed data wasn't smaller than the input.
     */
    public static Optional<ByteBuffer> compress(final ByteBuffer input) {
        final byte[] b = input.array();
        return compress(b, input.arrayOffset(), input.remaining());
    }

    /**
     * Compress some data as much as possible using the Gzip algorithm.
     * @param input the data to compress.
     * @param offset the offset within <code>input</code> of the data to compress.
     * @param length the length of the data within <code>input</code> to compress.
     * @return the compressed data (including Gzip container), or empty
     *      if the compressed data wasn't smaller than the input.
     */
    public static Optional<ByteBuffer> compress(final byte[] input,
                                                final int offset,
                                                final int length) {
        // Step 1: Perform the actual compression.
        final Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
        deflater.setInput(input, offset, length);
        deflater.finish();
        final byte[] payload = new byte[input.length - GZIP_FOOTER_SIZE];
        final int payloadSize = deflater.deflate(payload);
        deflater.end();
        // Step 2: Calculate the CRC for the trailer.
        final CRC32 checksum = new CRC32();
        checksum.update(input);
        // Step 3: Assemble the Gzip container with the compressed data.
        final Optional<ByteBuffer> output;
        final int outputSize = GZIP_OVERHEAD + payloadSize;
        if (outputSize < input.length) {
            final ByteBuffer bb = ByteBuffer.allocate(outputSize)
                                            .order(ByteOrder.LITTLE_ENDIAN);
            bb.put(GZIP_HEADER)
              .put(payload, 0, payloadSize)
              .putInt((int)checksum.getValue())
              .putInt(input.length);
            bb.flip();
            output = Optional.of(bb);
        } else {
            // The output wasn't smaller than the input.
            output = Optional.empty();
        }
        return output;
    }
}
