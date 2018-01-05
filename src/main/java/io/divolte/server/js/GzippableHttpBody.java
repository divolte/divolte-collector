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

package io.divolte.server.js;

import io.undertow.util.ETag;

import java.nio.ByteBuffer;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Value class for a HTTP body and its accompanying ETag.
 *
 * In addition, a Gzipped variant is available, so long as it is smaller
 * than the uncompressed version.
 */
@ParametersAreNonnullByDefault
public class GzippableHttpBody extends HttpBody {
    private static final Logger logger = LoggerFactory.getLogger(GzippableHttpBody.class);

    private final Optional<HttpBody> gzippedBody;

    public GzippableHttpBody(final ByteBuffer data, final ETag eTag) {
        super(data, eTag);
        logger.debug("Compressing resource.");
        final Optional<ByteBuffer> gzippedData = Gzip.compress(data);
        if (gzippedData.isPresent()) {
            logger.info("Compressed resource: {} -> {}",
                        data.remaining(), gzippedData.get().remaining());
            gzippedBody = Optional.of(new HttpBody(gzippedData.get(),
                                                   new ETag(eTag.isWeak(), "gz+" + eTag.getTag())));
        } else {
            logger.info("Resource not compressable.");
            gzippedBody = Optional.empty();
        }
    }

    public Optional<HttpBody> getGzippedBody() {
        return gzippedBody;
    }
}
