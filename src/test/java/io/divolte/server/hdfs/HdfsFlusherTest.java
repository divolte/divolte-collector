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

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.ValidatedConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.junit.Assert.*;

public class HdfsFlusherTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfsFlusherTest.class);

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
    public void shouldCreateAndPopulateFileWithSimpleStrategy() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
            ConfigFactory.parseString(
                   "divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every = 1 day\n"
                   + "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                   + "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                   .withFallback(ConfigFactory.parseResources("hdfs-flusher-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final List<Record> records = LongStream.range(0, 10)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        flusher.cleanup();

        Files.walk(tempPublishDir)
        .filter((p) -> p.toString().endsWith(".avro"))
        .findFirst()
        .ifPresent((p) -> verifyAvroFile(records, schema, p));
    }

    @Test
    public void shouldWriteInProgressFilesWithNonAvroExtension() throws IOException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                             "divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every = 1 day\n"
                             + "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                             + "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                          .withFallback(ConfigFactory.parseResources("hdfs-flusher-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        final List<Record> records = LongStream.range(0, 10)
                                         .mapToObj((time) -> new GenericRecordBuilder(schema)
                                                 .set("ts", time)
                                                 .set("remoteHost", ARBITRARY_IP)
                                                 .build())
                                         .collect(Collectors.toList());

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        assertTrue(Files.walk(tempInflightDir)
             .filter((p) -> p.toString().endsWith(".avro.partial"))
             .findFirst()
             .isPresent());
    }

    @Test
    public void shouldRollFilesWithSimpleStrategy() throws IOException, InterruptedException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
             ConfigFactory.parseString(
                             "divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every = 1 second\n"
                             + "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                             + "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                          .withFallback(ConfigFactory.parseResources("hdfs-flusher-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final List<Record> records = LongStream.range(0, 5)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        for (int c = 0; c < 2; c++) {
            Thread.sleep(500);
            flusher.heartbeat();
        }

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        flusher.cleanup();

        final MutableInt count = new MutableInt(0);
        Files.walk(tempPublishDir)
        .filter((p) -> p.toString().endsWith(".avro"))
        .forEach((p) -> {
            verifyAvroFile(records, schema, p);
            count.increment();
        });

        assertEquals(2, count.intValue());
    }

    @Test
    public void shouldNotCreateEmptyFiles() throws IOException, InterruptedException {
        final Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        final Config config =
            ConfigFactory.parseString(
                            "divolte.hdfs_flusher.simple_rolling_file_strategy.roll_every = 100 millisecond\n"
                            + "divolte.hdfs_flusher.simple_rolling_file_strategy.working_dir = \"" + tempInflightDir.toString() + "\"\n"
                            + "divolte.hdfs_flusher.simple_rolling_file_strategy.publish_dir = \"" + tempPublishDir.toString() + '"')
                         .withFallback(ConfigFactory.parseResources("hdfs-flusher-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        final List<Record> records = LongStream.range(0, 5)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        final HdfsFlusher flusher = new HdfsFlusher(vc, schema);

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        for (int c = 0; c < 4; c++) {
            Thread.sleep(500);
            flusher.heartbeat();
        }

        records.forEach((record) -> flusher.process(AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(), DivolteIdentifier.generate(), System.currentTimeMillis(), 0, record)));

        flusher.cleanup();

        final MutableInt count = new MutableInt(0);
        Files.walk(tempPublishDir)
        .filter((p) -> p.toString().endsWith(".avro"))
        .forEach((p) -> {
            verifyAvroFile(records, schema, p);
            count.increment();
        });

        assertEquals(2, count.intValue());
    }


    private void deleteQuietly(Path p) {
        try {
            Files.delete(p);
        } catch (final Exception e) {
            logger.debug("Ignoring failure while deleting file: " + p, e);
        }
    }

    private void verifyAvroFile(List<Record> expected, Schema schema, Path avroFile) {
        final List<Record> result =
                StreamSupport
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
