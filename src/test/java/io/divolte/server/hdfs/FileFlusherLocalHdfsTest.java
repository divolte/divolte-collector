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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.annotation.ParametersAreNonnullByDefault;

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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.Item;

@ParametersAreNonnullByDefault
public class HdfsFlusherTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfsFlusherTest.class);

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String ARBITRARY_IP = "8.8.8.8";

    private Schema schema;
    private Path tempInflightDir;
    private Path tempPublishDir;

    private List<Record> records;
    private HdfsFlusher flusher;

    @Before
    public void setup() throws IOException {
        schema = schemaFromClassPath("/MinimalRecord.avsc");
        tempInflightDir = Files.createTempDirectory("hdfs-flusher-test-inflight");
        tempPublishDir = Files.createTempDirectory("hdfs-flusher-test-publish");
    }

    @After
    public void teardown() throws IOException {
        schema = null;

        Files.walk(tempInflightDir)
            .filter((p) -> !p.equals(tempInflightDir))
            .forEach(this::deleteQuietly);
        deleteQuietly(tempInflightDir);
        tempInflightDir = null;

        Files.walk(tempPublishDir)
            .filter((p) -> !p.equals(tempPublishDir))
            .forEach(this::deleteQuietly);
        deleteQuietly(tempPublishDir);
        tempPublishDir = null;

        flusher = null;
        records = null;
        flusher = null;
    }

    @Test
    public void shouldCreateAndPopulateFileWithSimpleStrategy() throws IOException {
        setupFlusher("1 day", 10);
        processRecords();

        flusher.cleanup();

        Files.walk(tempPublishDir)
             .filter((p) -> p.toString().endsWith(".avro"))
             .findFirst()
             .ifPresent((p) -> verifyAvroFile(records, schema, p));
    }

    @Test
    public void shouldWriteInProgressFilesWithNonAvroExtension() throws IOException {
        setupFlusher("1 day", 10);
        processRecords();

        assertTrue(Files.walk(tempInflightDir)
             .filter((p) -> p.toString().endsWith(".avro.partial"))
             .findFirst()
             .isPresent());
    }

    @Test
    public void shouldRollFilesWithSimpleStrategy() throws IOException, InterruptedException {
        setupFlusher("1 second", 5);
        processRecords();

        for (int c = 0; c < 2; c++) {
            Thread.sleep(500);
            flusher.heartbeat();
        }

        processRecords();

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
        setupFlusher("100 millisecond", 5);

        processRecords();

        for (int c = 0; c < 4; c++) {
            Thread.sleep(500);
            flusher.heartbeat();
        }

        processRecords();

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

    private void setupFlusher(final String rollEvery, final int recordCount) throws IOException {
        final Config config = ConfigFactory
                .parseMap(ImmutableMap.of(
                        "divolte.sinks.hdfs.file_strategy.roll_every", rollEvery,
                        "divolte.sinks.hdfs.file_strategy.working_dir", tempInflightDir.toString(),
                        "divolte.sinks.hdfs.file_strategy.publish_dir", tempPublishDir.toString()))
                .withFallback(ConfigFactory.parseResources("hdfs-flusher-test.conf"))
                .withFallback(ConfigFactory.parseResources("reference-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        records = LongStream.range(0, recordCount)
                            .mapToObj((time) ->
                                    new GenericRecordBuilder(schema)
                                      .set("ts", time)
                                      .set("remoteHost", ARBITRARY_IP)
                                      .build())
                            .collect(Collectors.toList());

        flusher = new HdfsFlusher(vc, "hdfs", schema);
    }

    private void processRecords() {
        records.stream().map(
                (record) -> AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(),
                                                        DivolteIdentifier.generate(),
                                                        record))
                        .forEach((arb) -> flusher.process(Item.of(0, arb.getPartyId().value, arb)));
    }

    private void deleteQuietly(final Path p) {
        try {
            Files.delete(p);
        } catch (final Exception e) {
            logger.debug("Ignoring failure while deleting file: " + p, e);
        }
    }

    private void verifyAvroFile(final List<Record> expected, final Schema schema, final Path avroFile) {
        final List<Record> result =
                StreamSupport
                    .stream(readAvroFile(schema, avroFile.toFile()).spliterator(), false)
                    .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    private DataFileReader<Record> readAvroFile(final Schema schema, final File file) {
        final DatumReader<Record> dr = new GenericDatumReader<>(schema);
        try {
            return new DataFileReader<>(file, dr);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema schemaFromClassPath(final String resource) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return new Schema.Parser().parse(resourceStream);
        }
    }
}
