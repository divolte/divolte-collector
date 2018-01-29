package io.divolte.server.filesinks.gcs;

import java.io.IOException;

public class RetriableIOException extends IOException {
    private static final long serialVersionUID = 6721338145700189458L;

    public RetriableIOException() {
        super();
    }

    public RetriableIOException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RetriableIOException(final String message) {
        super(message);
    }

    public RetriableIOException(final Throwable cause) {
        super(cause);
    }
}
