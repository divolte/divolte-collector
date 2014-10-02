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
