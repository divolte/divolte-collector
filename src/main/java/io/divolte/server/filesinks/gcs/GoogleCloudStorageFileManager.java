package io.divolte.server.filesinks.gcs;

import java.io.IOException;

import org.apache.avro.Schema;

import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.filesinks.FileManager;

public class GoogleCloudStorageFileManager implements FileManager {

    @Override
    public DivolteFile createFile(final String name) throws IOException {
        return null;
    }

    public static FileManagerFactory newFactory(final ValidatedConfiguration configuration, final String sinkName, final Schema schema) {
        return new GoogleCloudStorageFileManagerFactory();
    }

    public static class GoogleCloudStorageFileManagerFactory implements FileManagerFactory {
        @Override
        public void verifyFileSystemConfiguration() {
        }

        @Override
        public FileManager create() {
            return null;
        }
    }
}



/* Dump of previous GCS code to keep around for implementation direction:

// TODO: Reimplement this class; this is basically just a proof of concept.

private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageFlusher.class);

private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyLLddHHmmss", Locale.ROOT);

private final HttpURLConnection connection;
private DataFileWriter<GenericRecord> writer;
private int counter;

GoogleCloudStorageFlusher(final ValidatedConfiguration config, final String name, final Schema schema) {
    try {
        final GoogleCredentials googleCredentials =
            GoogleCredentials.getApplicationDefault()
                             .createScoped(Collections.singletonList("https://www.googleapis.com/auth/devstorage.read_write"));
        final String fileName = "my-little-divolte-" + TIME_FORMATTER.format(Instant.now().atZone(ZoneOffset.UTC)) + ".avro";
        final URL destination = new URL("https://www.googleapis.com/upload/storage/v1/b/andrew-snare-tinkering/o?uploadType=media&name=" + fileName);
        connection = (HttpURLConnection) destination.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setAllowUserInteraction(false);
        connection.setRequestProperty("content-type", "application/octet-stream");
        googleCredentials.getRequestMetadata().entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(v -> Maps.immutableEntry(entry.getKey(), v)))
            .forEach(entry -> connection.addRequestProperty(entry.getKey(), entry.getValue()));
        final OutputStream rawUpload = connection.getOutputStream();
        rawUpload.flush();

        writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, rawUpload);
        writer.setFlushOnEveryBlock(true);
    } catch (final IOException e) {
        throw new RuntimeException(e);
    }
}

@Override
public ProcessingDirective process(Item<AvroRecordBuffer> event) {
    try {
        writer.appendEncoded(event.payload.getByteBuffer());
        writer.flush();
    } catch (final IOException e) {
        throw new RuntimeException(e);
    }
    return ProcessingDirective.CONTINUE;
}

@Override
public void cleanup() {
    try {
        writer.close();
        logger.debug("Response code: {}", connection.getResponseCode());
        final String response = CharStreams.toString(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
        logger.debug("Response body: {}", response);
    } catch (final IOException e) {
        throw new RuntimeException(e);
    }
}

*/