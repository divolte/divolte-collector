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

import io.undertow.util.ETag;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Value class for a HTTP body and its accompanying ETag.
 */
@ParametersAreNonnullByDefault
public class HttpBody {
    private final ByteBuffer body;
    private final ETag eTag;

    public HttpBody(final ByteBuffer body, final ETag eTag) {
        this.body = Objects.requireNonNull(body.asReadOnlyBuffer());
        this.eTag = Objects.requireNonNull(eTag);
    }

    public ByteBuffer getBody() {
        return body.duplicate();
    }

    public ETag getETag() {
        return eTag;
    }
}
