package io.divolte.server.hdfs;

import static org.junit.Assert.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.CookieValues;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

    private Path tempDir;

    @Before
    public void setupTempDir() throws IOException {
        tempDir = Files.createTempDirectory("hdfs-flusher-binning-test");
    }

    @After
    public void cleanupTempDir() throws IOException {
        Files.walk(tempDir)
        .filter((p) -> !p.equals(tempDir))
        .forEach(this::deleteQuietly);
        deleteQuietly(tempDir);
    }

    @Test
    public void shouldCreateFilePerRound() throws IOException {
        Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        Config config = ConfigFactory.parseResources("hdfs-flusher-binning-test.conf").withFallback(ConfigFactory.parseString(
                "divolte.hdfs_flusher.session_binning_file_strategy.dir = \"" + tempDir.toString() + "\""));

        HdfsFlusher flusher = new HdfsFlusher(config, schema);

        List<Record> records = LongStream.range(0, 5)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time * 1000 + 100)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                CookieValues.generate((Long) record.get("ts")),
                                CookieValues.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                record)));

        flusher.cleanup();

        // abuse AtomicInteger as mutable integer object for a final counter
        final AtomicInteger count = new AtomicInteger(0);
        Files.walk(tempDir)
        .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
        .filter((p) -> p.toString().endsWith(".avro"))
        .forEach((p) -> verifyAvroFile(records.subList(count.get(), count.incrementAndGet()), schema, p));
    }

    @Test
    public void eventsShouldStickWithSessionStartTimeRound() throws IOException {
        Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        Config config = ConfigFactory.parseResources("hdfs-flusher-binning-test.conf").withFallback(ConfigFactory.parseString(
                "divolte.hdfs_flusher.session_binning_file_strategy.dir = \"" + tempDir.toString() + "\""));

        HdfsFlusher flusher = new HdfsFlusher(config, schema);

        List<Record> records = LongStream.range(0, 2)
        .mapToObj((time) -> new GenericRecordBuilder(schema)
        .set("ts", time * 1000 + 100)
        .set("remoteHost", ARBITRARY_IP)
        .build())
        .collect(Collectors.toList());

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                CookieValues.generate((Long) record.get("ts")),
                                CookieValues.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                record)));

        records.forEach(
                (record) -> flusher.process(
                        AvroRecordBuffer.fromRecord(
                                CookieValues.generate((Long) record.get("ts")),
                                CookieValues.generate((Long) record.get("ts")),
                                (Long) record.get("ts"),
                                record)));

        flusher.cleanup();

        List<Path> avroFiles = Files.walk(tempDir)
        .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
        .filter((p) -> p.toString().endsWith(".avro"))
        .collect(Collectors.toList());

        verifyAvroFile(Arrays.asList(records.get(0), records.get(0)), schema, avroFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(1), records.get(1)), schema, avroFiles.get(1));

        assertEquals(2, avroFiles.size());
    }

    @Test
    public void eventsShouldMoveToNextRoundFileIfSessionStartTimeRoundFileIsNoLongerOpen() throws IOException {
        Schema schema = schemaFromClassPath("/MinimalRecord.avsc");
        Config config = ConfigFactory.parseResources("hdfs-flusher-binning-test.conf").withFallback(ConfigFactory.parseString(
                "divolte.hdfs_flusher.session_binning_file_strategy.dir = \"" + tempDir.toString() + "\""));

        HdfsFlusher flusher = new HdfsFlusher(config, schema);

        List<Record> records = Arrays.asList(
                new GenericRecordBuilder(schema).set("ts", 100L).set("session", CookieValues.generate(100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 1100L).set("session", CookieValues.generate(1100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 2100L).set("session", CookieValues.generate(2100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3100L).set("session", CookieValues.generate(3100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3150L).set("session", CookieValues.generate(100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3160L).set("session", CookieValues.generate(1100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3170L).set("session", CookieValues.generate(2100).value).set("remoteHost", ARBITRARY_IP).build(),
                new GenericRecordBuilder(schema).set("ts", 3180L).set("session", CookieValues.generate(3100).value).set("remoteHost", ARBITRARY_IP).build()
                );

        List<AvroRecordBuffer> buffers = records
        .stream()
        .map((r) -> AvroRecordBuffer.fromRecord(CookieValues.generate(), CookieValues.tryParse((String) r.get("session")).get(), (Long) r.get("ts"), r))
        .collect(Collectors.toList());

        buffers.forEach((b) -> {
            flusher.process(b);
        });
        flusher.cleanup();

        List<Path> avroFiles = Files.walk(tempDir)
        .sorted((l, r) -> l.toString().compareTo(r.toString())) // files sort lexicographically in time order
        .filter((p) -> p.toString().endsWith(".avro"))
        .collect(Collectors.toList());

        verifyAvroFile(Arrays.asList(records.get(0)), schema, avroFiles.get(0));
        verifyAvroFile(Arrays.asList(records.get(1), records.get(4), records.get(5)), schema, avroFiles.get(1));
        verifyAvroFile(Arrays.asList(records.get(2), records.get(6)), schema, avroFiles.get(2));
        verifyAvroFile(Arrays.asList(records.get(3), records.get(7)), schema, avroFiles.get(3));

        assertEquals(4, avroFiles.size());
    }

    private void deleteQuietly(Path p) {
        try {
            Files.delete(p);
        } catch (final Exception e) {
            logger.info("Ignoring failure while deleting file: " + p, e);
        }
    }

    private void verifyAvroFile(List<Record> expected, Schema schema, Path avroFile) {
        List<Record> result = StreamSupport.stream(readAvroFile(schema, avroFile.toFile()).spliterator(), false)
        .collect(Collectors.toList());

        assertEquals(expected, result);
    }

    private DataFileReader<Record> readAvroFile(Schema schema, File file) {
        DatumReader<Record> dr = new GenericDatumReader<>(schema);
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
