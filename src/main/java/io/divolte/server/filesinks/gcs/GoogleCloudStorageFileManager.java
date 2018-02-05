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

package io.divolte.server.filesinks.gcs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.jcabi.aspects.RetryOnFailure;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.GoogleCloudStorageSinkConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.filesinks.FileManager;
import io.divolte.server.filesinks.gcs.entities.ComposeRequest;
import io.divolte.server.filesinks.gcs.entities.ComposeRequest.SourceObject;
import io.divolte.server.filesinks.gcs.entities.GcsObjectResponse;
import io.divolte.server.filesinks.gcs.entities.GetBucketResponse;

public class GoogleCloudStorageFileManager implements FileManager {
    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudStorageFileManager.class);

    private static final ObjectMapper MAPPER;
    static {
        MAPPER = new ObjectMapper();
        MAPPER.registerModule(new ParameterNamesModule());
        MAPPER.registerModule(new Jdk8Module());
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static final String GET_BUCKET_URL_PREFIX = "https://www.googleapis.com/storage/v1/b/";
    private static final String UPLOAD_FILE_URL_TEMPLATE = "https://www.googleapis.com/upload/storage/v1/b/%s/o?uploadType=media&name=%s";
    private static final String COMPOSE_FILE_URL_TEMPLATE = "https://www.googleapis.com/storage/v1/b/%s/o/%s/compose";
    private static final String DELETE_FILE_URL_TEMPLATE = "https://www.googleapis.com/storage/v1/b/%s/o/%s";

    private static final char GCS_PATH_SEPARATOR_CHAR = '/';
    private static final String URL_ENCODING = "UTF-8";

    private static final String GCS_OAUTH_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";

    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final Map<String,String> JSON_CONTENT_TYPE_HEADER = ImmutableMap.of("Content-Type", JSON_CONTENT_TYPE);

    private static final String AVRO_CONTENT_TYPE = "application/octet-stream";
    private static final Map<String,String> AVRO_CONTENT_TYPE_HEADER = ImmutableMap.of("Content-Type", AVRO_CONTENT_TYPE);

    private static final String PART_CLASSIFIER = ".part";

    private final int recordBufferSize;
    private final Schema schema;
    private final String bucketEncoded;
    private final String inflightDir;
    private final String publishDir;

    public GoogleCloudStorageFileManager(final int recordBufferSize, final Schema schema, final String bucket, final String inflightDir, final String publishDir) {
        try {
            this.recordBufferSize = recordBufferSize;
            this.schema = Objects.requireNonNull(schema);
            this.bucketEncoded = URLEncoder.encode(bucket, URL_ENCODING);
            this.inflightDir = Objects.requireNonNull(inflightDir);
            this.publishDir = Objects.requireNonNull(publishDir);
        } catch (final UnsupportedEncodingException e) {
            // Should not happen. URL encoding the bucket and dirs is verified during
            // configuration verification.
            logger.error("Could not URL-encode bucket name.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public DivolteFile createFile(final String name) throws IOException {
        return new GoogleCloudStorageDivolteFile(name);
    }

    public static FileManagerFactory newFactory(final ValidatedConfiguration configuration, final String sinkName, final Schema schema) {
        return new GoogleCloudStorageFileManagerFactory(configuration, sinkName, schema);
    }

    public class GoogleCloudStorageDivolteFile implements DivolteFile {
        private final AvroRecordBuffer[] buffer;
        private final DataFileWriter<GenericRecord> writer;
        private final DynamicDelegatingOutputStream avroTargetStream;

        private final String inflightNameEncoded;
        private final String inflightPartialNameEncoded;
        private final String publishNameEncoded;

        private final String inflightName;
        private final String inflightPartialName;

        private boolean partWritten;
        private int position;

        private GoogleCloudStorageDivolteFile(final String fileName) throws IOException {
            /*
             * Consider pooling these or assume only one file to be active at any point in
             * time and use a single buffer per outer instance. While the latter is
             * currently valid, new file syncing and rolling strategies might change this.
             */
            this.buffer = new AvroRecordBuffer[recordBufferSize];

            this.inflightName = inflightDir + GCS_PATH_SEPARATOR_CHAR + fileName;
            this.inflightPartialName = inflightName + PART_CLASSIFIER;

            this.inflightNameEncoded =  URLEncoder.encode(inflightName, URL_ENCODING);
            this.inflightPartialNameEncoded = inflightNameEncoded + PART_CLASSIFIER;
            this.publishNameEncoded =  URLEncoder.encode(publishDir + GCS_PATH_SEPARATOR_CHAR + fileName, URL_ENCODING);

            final URL remoteFileUrl = uploadUrlFor(bucketEncoded, inflightNameEncoded);

            final HttpURLConnection connection = setupUrlConnection(POST, remoteFileUrl, true, AVRO_CONTENT_TYPE_HEADER);
            final OutputStream os = connection.getOutputStream();

            /*
             * We create a single Avro writer, but write parts of the Avro stream to
             * multiple files, which are composed into a single file after flushing files.
             * We use a DynamicDelegatingOutputStream for this, which is an output stream
             * wrapper that supports changing the wrapped stream on the fly.
             *
             * When creating an Avro writer, it immediately writes the Avro header to the
             * underlying stream. As such, we need to open the HTTP connection before
             * creating the writer. As the writer is final, we cannot use the
             * googlePost(...) helper in the constructor, because we cannot set a final from
             * a lambda.
             */
            avroTargetStream = new DynamicDelegatingOutputStream();
            avroTargetStream.attachDelegate(os);
            try {
                writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, avroTargetStream);
                writer.flush();
            } finally {
                avroTargetStream.detachDelegate();
            }
            os.close();


            final GcsObjectResponse response = parseResponse(GcsObjectResponse.class, connection);
            logger.debug("Google Cloud Storage upload response {}", response);

            partWritten = false;
        }

        @Override
        public void append(final AvroRecordBuffer record) throws IOException {
            /*
             * Hang on to buffer; write later on sync. We don't guard against overflow, as
             * the buffer is allocated to the max configured number of inflight records
             * between syncing for the file strategy.
             */
            buffer[position++] = record;
        }

        @Override
        public void sync() throws IOException {
            writeBufferAndComposeParts(inflightNameEncoded);
        }

        @Override
        public void closeAndPublish() throws IOException {
            // write final part and compose all parts into published file
            writeBufferAndComposeParts(publishNameEncoded);

            // delete inflight partial
            googleDelete(deleteUrlFor(bucketEncoded, inflightPartialNameEncoded));

            // delete inflight composed
            googleDelete(deleteUrlFor(bucketEncoded, inflightNameEncoded));
        }

        @Override
        public void discard() throws IOException {
            // best effort to delete partial file
            if (partWritten) {
                googleDelete(deleteUrlFor(bucketEncoded, inflightPartialNameEncoded));
            }
            googleDelete(deleteUrlFor(bucketEncoded, inflightNameEncoded));
        }

        private void writeBufferAndComposeParts(final String composeDestinationObjectEncoded) throws MalformedURLException, IOException {
            final ImmutableList<SourceObject> sourcesToCompose;

            if (position > 0) {
                final URL partUploadUrl = uploadUrlFor(bucketEncoded, inflightPartialNameEncoded);
                final GcsObjectResponse uploadResponse = googlePost(partUploadUrl, GcsObjectResponse.class, AVRO_CONTENT_TYPE_HEADER, os -> {
                    avroTargetStream.attachDelegate(os);
                    try {
                        for (int c = 0; c < position; c++) {
                            // Write Avro record buffer to file
                            writer.appendEncoded(buffer[c].getByteBuffer());

                            // Clear (our) reference to flushed buffer
                            buffer[c] = null;
                        }
                        writer.flush();
                    } finally {
                        avroTargetStream.detachDelegate();
                    }
                });
                partWritten = true;
                position = 0;

                logger.debug("Google Cloud Storage upload response {}", uploadResponse);

                // New part was written; compose two parts.
                sourcesToCompose = ImmutableList.of(
                        new SourceObject(inflightName),
                        new SourceObject(inflightPartialName));
            } else {
                // Nothing was written; compose with itself, potentially to a new destination.
                sourcesToCompose = ImmutableList.of(
                        new SourceObject(inflightName));
            }

            final ComposeRequest composeRequest = new ComposeRequest(
                    new ComposeRequest.DestinationObject(AVRO_CONTENT_TYPE),
                    sourcesToCompose);

            final URL composeUrl = composeUrlFor(bucketEncoded, composeDestinationObjectEncoded);
            final GcsObjectResponse composeResponse =
                googlePost(composeUrl, GcsObjectResponse.class, JSON_CONTENT_TYPE_HEADER,
                           os -> MAPPER.writeValue(os, composeRequest));

            logger.debug("Google Cloud Storage compose response {}", composeResponse);
        }
    }

    public static class GoogleCloudStorageFileManagerFactory implements FileManagerFactory {
        private final Schema schema;
        private final ValidatedConfiguration configuration;
        private final String name;

        private GoogleCloudStorageFileManagerFactory(final ValidatedConfiguration vc, final String sinkName, final Schema schema) {
            this.schema = Objects.requireNonNull(schema);
            this.configuration = Objects.requireNonNull(vc);
            this.name = Objects.requireNonNull(sinkName);
        }

        @Override
        public void verifyFileSystemConfiguration() {
            // Just perform a get on the bucket for access verification
            try {
                /*
                 * Get the credentials. This is a redundant operation, just to provide a nicer
                 * error message if the presence of default credentials are the issue instead of
                 * the actual connection / ACLs / bucket existence.
                 */
                getGoogleCredentials();
            } catch (final IOException ioe) {
                logger.error("Failed to obtain application default credentials for Google Cloud Storage for OAuth scope '" + GoogleCloudStorageFileManager.GCS_OAUTH_SCOPE + "'", ioe);
                throw new UncheckedIOException("Could not obtain application default credentials for Google Cloud Storage", ioe);
            }

            final GoogleCloudStorageSinkConfiguration sinkConfiguration = configuration.configuration().getSinkConfiguration(name, GoogleCloudStorageSinkConfiguration.class);
            try {
                // Perform a GET on the bucket
                final URL bucketUrl = new URL(GoogleCloudStorageFileManager.GET_BUCKET_URL_PREFIX + URLEncoder.encode(sinkConfiguration.bucket, URL_ENCODING));
                final GetBucketResponse response = googleGet(bucketUrl, GetBucketResponse.class);
                logger.info("Google Cloud Storage sink {} using bucket {}", name, response);

                // Additionally, make sure that the working dir and publish dir are URL
                // encodeable. Both of these throw a descendant of IOException on failure.
                URLEncoder.encode(sinkConfiguration.fileStrategy.workingDir, URL_ENCODING);
                URLEncoder.encode(sinkConfiguration.fileStrategy.publishDir, URL_ENCODING);
            } catch (final IOException ioe) {
                logger.error("Failed to fetch bucket information for Google Cloud Storage sink {} using bucket {}. Assuming destination unwritable.", name, sinkConfiguration.bucket);
                throw new UncheckedIOException(ioe);
            }
        }

        @Override
        public FileManager create() {
            final GoogleCloudStorageSinkConfiguration sinkConfiguration = configuration.configuration().getSinkConfiguration(name, GoogleCloudStorageSinkConfiguration.class);
            return new GoogleCloudStorageFileManager(sinkConfiguration.fileStrategy.syncFileAfterRecords, schema, sinkConfiguration.bucket,
                    sinkConfiguration.fileStrategy.workingDir, sinkConfiguration.fileStrategy.publishDir);
        }
    }

    private static GoogleCredentials getGoogleCredentials() throws IOException {
        return GoogleCredentials.getApplicationDefault()
                         .createScoped(Collections.singletonList(GoogleCloudStorageFileManager.GCS_OAUTH_SCOPE));
    }

    @RetryOnFailure(attempts = 3, delay = 2, unit = TimeUnit.SECONDS)
    private static <T> T googlePost(final URL url, final Class<T> resultType, final Map<String,String> additionalHeaders, final BodyWriter writer) throws IOException {
        final HttpURLConnection connection = setupUrlConnection(POST, url, true, additionalHeaders);

        final OutputStream os = connection.getOutputStream();
        writer.write(os);
        os.flush();
        os.close();

        return parseResponse(resultType, connection);
    }

    private interface BodyWriter {
        void write(final OutputStream stream) throws IOException;
    }

    @RetryOnFailure(attempts = 3, delay = 2, unit = TimeUnit.SECONDS)
    private static <T> T googleGet(final URL url, final Class<T> resultType) throws IOException {
        return parseResponse(resultType, setupUrlConnection(GET, url, false, Collections.emptyMap()));
    }

    @RetryOnFailure(attempts = 3, delay = 2, unit = TimeUnit.SECONDS)
    private static void googleDelete(final URL url) throws IOException {
        final HttpURLConnection connection = setupUrlConnection(DELETE, url, false, Collections.emptyMap());

        throwIOExceptionOnErrorResponse(connection);

        final InputStream stream = connection.getInputStream();
        /*
         * As per the docs, Google sends a empty response with a 200. We need to get and
         * drain the stream for the HTTP client to consider the connection for reuse.
         */
        ByteStreams.exhaust(stream);
    }

    private static <T> T parseResponse(final Class<T> resultType, final HttpURLConnection connection) throws IOException, JsonParseException, JsonMappingException {
        throwIOExceptionOnErrorResponse(connection);

        final InputStream stream = connection.getInputStream();
        final T response = MAPPER.readValue(stream, resultType);
        stream.close();
        return response;
    }

    private static void throwIOExceptionOnErrorResponse(final HttpURLConnection connection) throws IOException {
        final int responseCode = connection.getResponseCode();
        // Note: the docs are specific about closing the streams after reading in order
        // to trigger proper Keep-Alive usage
        if (responseCode < 200 || responseCode > 299) {
            final InputStream stream = connection.getErrorStream();
            // Read the error response as String; GCS sometimes sends a JSON error response
            // and sometimes text/plain (e.g. "Not found.")
            final String response = CharStreams.toString(new InputStreamReader(stream, URL_ENCODING));
            stream.close();
            logger.error("Received unexpected response from Google Cloud Storage. Response status code: {}. Response body: {}", responseCode, response);
            throw new IOException("Received unexpected response from Google Cloud Storage.");
        }
    }

    private static HttpURLConnection setupUrlConnection(final String method, final URL url, final boolean write, final Map<String,String> additionalHeaders) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setAllowUserInteraction(false);

        getGoogleCredentials()
            .getRequestMetadata()
            .forEach(
                    (headerName, headerValues) -> headerValues
                        .forEach(
                                value -> connection.addRequestProperty(headerName, value))
                        );

        additionalHeaders.forEach(connection::addRequestProperty);

        connection.setRequestMethod(method);
        connection.setDoInput(true);
        connection.setDoOutput(write);

        return connection;
    }

    private static URL uploadUrlFor(final String bucketPart, final String namePart) throws MalformedURLException {
        return new URL(String.format(UPLOAD_FILE_URL_TEMPLATE, bucketPart, namePart));
    }

    private static URL composeUrlFor(final String bucketPart, final String namePart) throws MalformedURLException {
        return new URL(String.format(COMPOSE_FILE_URL_TEMPLATE, bucketPart, namePart));
    }

    private static URL deleteUrlFor(final String bucketPart, final String namePart) throws MalformedURLException {
        return new URL(String.format(DELETE_FILE_URL_TEMPLATE, bucketPart, namePart));
    }
}
