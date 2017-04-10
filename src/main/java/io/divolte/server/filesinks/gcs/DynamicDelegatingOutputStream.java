package io.divolte.server.filesinks.gcs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Preconditions;

/*
 * OutputStream wrapper that allows to dynamically replace the wrapped stream.
 */
@ParametersAreNonnullByDefault
public class DynamicDelegatingOutputStream extends OutputStream {
    private OutputStream wrapped;

    public DynamicDelegatingOutputStream() {
        wrapped = null;
    }

    public void detachDelegate() throws IOException {
        wrapped.flush();
        wrapped = null;
    }

    public void attachDelegate(final OutputStream newDelegate) {
        wrapped = Objects.requireNonNull(newDelegate);
    }

    @Override
    public void write(final int b) throws IOException {
        Preconditions.checkState(wrapped != null, "Dynamic delegating stream not currently attached.");
        wrapped.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        Preconditions.checkState(wrapped != null, "Dynamic delegating stream not currently attached.");
        wrapped.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        Preconditions.checkState(wrapped != null, "Dynamic delegating stream not currently attached.");
        wrapped.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        Preconditions.checkState(wrapped != null, "Dynamic delegating stream not currently attached.");
        wrapped.flush();
    }

    @Override
    public void close() throws IOException {
        Preconditions.checkState(wrapped != null, "Dynamic delegating stream not currently attached.");
        wrapped.close();
    }
}
