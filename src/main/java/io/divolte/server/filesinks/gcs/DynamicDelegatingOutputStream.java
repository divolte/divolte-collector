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

package io.divolte.server.filesinks.gcs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Preconditions;

/*
 * OutputStream wrapper that allows to dynamically replace the wrapped stream.
 */
@ParametersAreNonnullByDefault
public class DynamicDelegatingOutputStream extends OutputStream {
    @Nullable private OutputStream wrapped;

    public DynamicDelegatingOutputStream() {
        wrapped = null;
    }

    public void detachDelegate() throws IOException {
        Preconditions.checkState(wrapped != null);
        wrapped.flush();
        wrapped = null;
    }

    public void attachDelegate(final OutputStream newDelegate) {
        Preconditions.checkState(wrapped == null);
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
