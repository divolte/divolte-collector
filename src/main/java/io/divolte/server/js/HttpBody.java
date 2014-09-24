package io.divolte.server.js;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Value class for a HTTP body and its accompanying ETag.
 */
@ParametersAreNonnullByDefault
public class HttpBody {
    private final ByteBuffer body;
    private final String eTag;

    public HttpBody(final ByteBuffer body, final String eTag) {
        this.body = Objects.requireNonNull(body.asReadOnlyBuffer());
        this.eTag = Objects.requireNonNull(eTag);
    }

    public ByteBuffer getBody() {
        return body.duplicate();
    }

    public String getETag() {
        return eTag;
    }
}
