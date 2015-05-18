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

package io.divolte.server.hdfs;

import static org.junit.Assert.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.ValidatedConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SessionBinningFileStrategyTest {
    private static final Logger logger = LoggerFactory.getLogger(SessionBinningFileStrategyTest.class);

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String ARBITRARY_IP = "8.8.8.8";

    private Path tempInflightDir;
    private Path tempPublishDir;

    @Before
    public void setupTempDir() throws IOException {
        tempInflightDir = Files.createTempDirectory("hdfs-flusher-test-inflight");
        tempPublishDir = Files.createTempDirectory("hdfs-flusher-test-publish");
    }

    @After
    public void cleanupTempDir() throws IOException {
        Files.walk(tempInflightDir)
             .filter((p) -> !p.equals(tempInflightDir))
             .forEach(this::deleteQuietly);
        deleteQuietly(tempInflightDir);
        Files.walk(tempPublishDir)
             .filter((p) -> !p.equals(tempPublishDir))
             .forEach(this::deleteQuietly);
        deleteQuietly(tempPublishDir);
    }

    @Test
    public void shouldCreateFilePerRound() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                             "divolte.hdfs_flusher.session_binning_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                             + "divolte.hdfs_flusher.session_binning_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                             .withFallback(ConfigFactory.parseResources("hdfs-flusher-binning-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final List<Record> records = LongStream.range(0, 5)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time * 1000 + 100)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                0,
                                record)));

        flusher.cleanup();

        final List<Path> inflightFiles = Files.walk(tempInflightDir)
                .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
                .filter((p) -> p.toString().endsWith(".avro.partial"))
                .collect(Collectors.toList());
        final List<Path> publishedFiles = Files.walk(tempPublishDir)
                .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
                .filter((p) -> p.toString().endsWith(".avro"))
                .collect(Collectors.toList());

        /*
         * We created 5 events, each in a different round. On each sync event, we evaluate
         * which open files can be closed because their 3-session span has elapsed. So:
         * a) On the 4th event, the 1st span is completed.
         * b) On the 5th event, the 2nd span is completed.
         * c) The last 3 spans remain in-flight.
         */
        assertEquals(3, inflightFiles.size());
        assertEquals(2 ,publishedFiles.size());
        verifyAvroFile(Arrays.asList(records.get(0)), schema, publishedFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(1)), schema, publishedFiles.get(1));
        verifyAvroFile(Arrays.asList(records.get(2)), schema, inflightFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(3)), schema, inflightFiles.get(1));
        verifyAvroFile(Arrays.asList(records.get(4)), schema, inflightFiles.get(2));
    }

    @Test
    public void eventsShouldStickWithSessionStartTimeRound() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                             "divolte.hdfs_flusher.session_binning_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                             + "divolte.hdfs_flusher.session_binning_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                             .withFallback(ConfigFactory.parseResources("hdfs-flusher-binning-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final List<Record> records = LongStream.range(0, 2)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time * 1000 + 100)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                0,
                                record)));

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                0,
                                record)));

        flusher.cleanup();

        final List<Path> avroFiles = Files.walk(tempInflightDir)
        .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
        .filter((p) -> p.toString().endsWith(".avro.partial"))
        .collect(Collectors.toList());

        assertEquals(2, avroFiles.size());
        verifyAvroFile(Arrays.asList(records.get(0), records.get(0)), schema, avroFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(1), records.get(1)), schema, avroFiles.get(1));
    }

    @Test
    public void eventsShouldMoveToNextRoundFileIfSessionStartTimeRoundFileIsNoLongerOpen() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                       "divolte.hdfs_flusher.session_binning_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                       + "divolte.hdfs_flusher.session_binning_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                    .withFallback(ConfigFactory.parseResources("hdfs-flusher-binning-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final List<Record> records = Arrays.asList(
                new GenericRecordBuilder(schema).set("ts", 100L).set("session", DivolteIdentifier.generate(100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 1100L).set("session", DivolteIdentifier.generate(1100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 2100L).set("session", DivolteIdentifier.generate(2100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3100L).set("session", DivolteIdentifier.generate(3100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3150L).set("session", DivolteIdentifier.generate(100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3160L).set("session", DivolteIdentifier.generate(1100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3170L).set("session", DivolteIdentifier.generate(2100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3180L).set("session", DivolteIdentifier.generate(3100).value).set("remoteHost", ARBITRARY_IP).build()
                );

        final List<AvroRecordBuffer> buffers = records
        .stream()
        .map((r) -> AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.tryParse((String) r.get("session")).get(), (Long) r.get("ts"), 0, r))
        .collect(Collectors.toList());

        buffers.forEach(flusher::process);
        flusher.cleanup();

        final List<Path> inflightFiles = Files.walk(tempInflightDir)
            .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
            .filter((p) -> p.toString().endsWith(".avro.partial"))
            .collect(Collectors.toList());
        final List<Path> publishedFiles = Files.walk(tempPublishDir)
            .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
            .filter((p) -> p.toString().endsWith(".avro"))
            .collect(Collectors.toList());

        assertEquals(1, publishedFiles.size());
        assertEquals(3, inflightFiles.size());

        verifyAvroFile(Arrays.asList(records.get(0)), schema, publishedFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(1), records.get(4), records.get(5)), schema, inflightFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(2), records.get(6)), schema, inflightFiles.get(1));
        verifyAvroFile(Arrays.asList(records.get(3), records.get(7)), schema, inflightFiles.get(2));
    }

    @Test
    public void shouldNotPublishInflightFilesOnCleanup() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                             "divolte.hdfs_flusher.session_binning_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                             + "divolte.hdfs_flusher.session_binning_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                          .withFallback(ConfigFactory.parseResources("hdfs-flusher-binning-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final Record record = new GenericRecordBuilder(schema)
                .set("ts", 100L)
                .set("remoteHost", ARBITRARY_IP)
                .build();

        flusher.process(AvroRecordBuffer.fromRecord(
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                DivolteIdentifier.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                0,
                                record));
        flusher.cleanup();

        final List<Path> inflightFiles = Files.walk(tempInflightDir)
                .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
                .filter((p) -> p.toString().endsWith(".avro.partial"))
                .collect(Collectors.toList());
        final List<Path> publishedFiles = Files.walk(tempPublishDir)
                .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
                .filter((p) -> p.toString().endsWith(".avro"))
                .collect(Collectors.toList());

        assertEquals(1, inflightFiles.size());
        assertEquals(0 ,publishedFiles.size());
    }

    private void deleteQuietly(Path p) {
        try {
            Files.delete(p);
        } catch (final Exception e) {
            logger.info("Ignoring failure while deleting file: " + p, e);
        }
    }

    private void verifyAvroFile(List<Record> expected, Schema schema, Path avroFile) {
        final List<Record> result = StreamSupport
                .stream(readAvroFile(schema, avroFile.toFile()).spliterator(), false)
                .collect(Collectors.toList());

        assertEquals(expected, result);
    }

    private DataFileReader<Record> readAvroFile(Schema schema, File file) {
        final DatumReader<Record> dr = new GenericDatumReader<>(schema);
        try {
            return new DataFileReader<>(file, dr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema schemaFromClassPath(final String resource) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return new Schema.Parser().parse(resourceStream);
        }
    }
}
