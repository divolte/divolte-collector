package io.divolte.server.filesinks;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;

public abstract class FileSinkHelpers {
    protected Schema schemaFromClassPath(final String resource) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return new Schema.Parser().parse(resourceStream);
        }
    }
}
