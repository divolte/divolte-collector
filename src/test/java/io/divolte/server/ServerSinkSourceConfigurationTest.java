/*
 * Copyright 2015 GoDataDriven B.V.
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

package io.divolte.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.divolte.server.ServerTestUtils.TestServer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@ParametersAreNonnullByDefault
public class ServerSinkSourceConfigurationTest {

    private static final String BROWSER_EVENT_URL_TEMPLATE =
        "http://localhost:%d%s/csc-event?"
            + "p=0%%3Ai1t84hgy%%3A5AF359Zjq5kUy98u4wQjlIZzWGhN~GlG&"
            + "s=0%%3Ai1t84hgy%%3A95CbiPCYln_1e0a6rFvuRkDkeNnc6KC8&"
            + "v=0%%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF&"
            + "e=0%%3A1fF6GFGjDOQiEx_OxnTm_tl4BH91eGLF0&"
            + "c=i1t8q2b6&"
            + "n=f&"
            + "f=f&"
            + "l=http%%3A%%2F%%2Flocalhost%%3A8290%%2F&"
            + "i=1ak&"
            + "j=sj&"
            + "k=2&"
            + "w=uq&"
            + "h=qd&"
            + "t=pageView&"
            + "x=si9804";

    private final Set<Path> tempDirectories = new HashSet<>();
    private Optional<TestServer> testServer = Optional.empty();

    private void startServer(final String configResource,
                             final ImmutableMap<String,Object> extraProperties) {
        startServer(() ->  new TestServer(configResource, extraProperties));
    }

    private void startServer(final String configResource) {
        startServer(() -> new TestServer(configResource));
    }

    private void startServer() {
        startServer(TestServer::new);
    }

    private void startServer(final Supplier<TestServer> supplier) {
        stopServer();
        testServer = Optional.of(supplier.get());
    }

    public void stopServer() {
        testServer.ifPresent(testServer -> testServer.server.shutdown());
        testServer = Optional.empty();
    }

    public Path createTempDirectory() throws IOException {
        final Path newTempDirectory = Files.createTempDirectory("divolte-test");
        tempDirectories.add(newTempDirectory);
        return newTempDirectory;
    }

    public void cleanupTempDirectories() {
        tempDirectories.forEach(ServerSinkSourceConfigurationTest::deleteRecursively);
        tempDirectories.clear();
    }

    private void request() throws IOException {
        request("");
    }

    private void request(final String sourcePrefix) throws IOException {
        request(sourcePrefix, 200);
    }

    private void request(final String sourcePrefix, final int expectedResponseCode) throws IOException {
        final URL url = new URL(String.format(BROWSER_EVENT_URL_TEMPLATE, testServer.get().port, sourcePrefix));
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        assertEquals(expectedResponseCode, conn.getResponseCode());
    }

    @ParametersAreNonnullByDefault
    private static class AvroFileLocator {
        private static final Logger logger = LoggerFactory.getLogger(AvroFileLocator.class);

        private final Path directory;
        private final ImmutableSet<Path> existingFiles;

        private AvroFileLocator(final Path directory) throws IOException {
            this.directory = Objects.requireNonNull(directory);
            existingFiles = Files.list(directory)
                                 .filter(AvroFileLocator::isAvroFile)
                                 .collect(MoreCollectors.toImmutableSet());
        }

        private static boolean isAvroFile(final Path p) {
            return p.toString().endsWith(".avro");
        }

        private static Stream<GenericRecord> listRecords(final Path avroFile) {
            final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            logger.debug("Reading records from new Avro file: {}", avroFile);
            try (final FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile.toFile(), datumReader)) {
                final ImmutableList<GenericRecord> records = ImmutableList.copyOf(fileReader.iterator());
                logger.info("Read {} record(s) from new Avro file: {}", records.size(), avroFile);
                return records.stream();
            } catch (final IOException e) {
                throw new UncheckedIOException("Error reading records from file: " + avroFile, e);
            }
        }

        public Stream<GenericRecord> listNewRecords() throws IOException {
            return Files.list(directory)
                        .filter(candidate -> isAvroFile(candidate) && !existingFiles.contains(candidate))
                        .flatMap(AvroFileLocator::listRecords);
        }
    }

    @Test
    public void shouldRegisterDefaultBrowserSource() throws IOException, InterruptedException {
        // Test the default browser source that should be present by default.
        startServer();
        request();
        testServer.get().waitForEvent();
    }

    @Test
    public void shouldRegisterExplicitSourceOnly() throws IOException, InterruptedException {
        // Test that if an explicit source is supplied, the builtin defaults are not present.
        startServer("browser-source-explicit.conf");
        request("/a-prefix");
        testServer.get().waitForEvent();
        request("", 404);
    }

    @Test
    public void shouldSupportLongSourcePaths() throws IOException, InterruptedException {
        // Test that the browser sources work with different types of path.
        startServer("browser-source-long-prefix.conf");
        request("/a/multi/component/prefix");
        testServer.get().waitForEvent();
    }

    @Test
    public void shouldSupportMultipleBrowserSources() throws IOException, InterruptedException {
        // Test that multiple browser sources are supported.
        startServer("browser-source-multiple.conf");
        request("/path1");
        request("/path2");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
    }

    @Test
    public void shouldSupportUnusedSource() throws IOException {
        // Test that an unused source is still reachable.
        startServer("browser-source-unused.conf");
        request("/unused");
    }

    @Test
    public void shouldSupportDefaultSourceMappingSink() throws IOException, InterruptedException {
        // Test that with an out-of-the-box default configuration the default source, mapping and sink are present.
        startServer(TestServer::createTestServerWithDefaultNonTestConfiguration);
        final AvroFileLocator avroFileLocator = new AvroFileLocator(Paths.get("/tmp"));
        request();
        testServer.get().waitForEvent();
        // Stopping the server flushes the HDFS files.
        stopServer();
        // Now we can check the number of events that turned up in new files in /tmp.
        assertEquals("Wrong number of new events logged to /tmp",
                     1, avroFileLocator.listNewRecords().count());
    }

    @Test
    public void shouldOnlyRegisterExplicitSourceMappingSink() throws IOException, InterruptedException {
        // Test that if an explicit source-mapping-sink is supplied, the builtin defaults are not present.
        final AvroFileLocator defaultAvroFileLocator = new AvroFileLocator(Paths.get("/tmp"));
        final Path avroDirectory = createTempDirectory();
        startServer("mapping-configuration-explicit.conf", ImmutableMap.of(
                "divolte.sinks.test-hdfs-sink.file_strategy.working_dir", avroDirectory.toString(),
                "divolte.sinks.test-hdfs-sink.file_strategy.publish_dir", avroDirectory.toString()
        ));
        final AvroFileLocator explicitAvroFileLocator = new AvroFileLocator(avroDirectory);
        request();
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - The default location (/tmp) shouldn't have anything new.
        //   - Our explicit location should have a single record.
        assertFalse("Default location (/tmp) shouldn't have any new logged events.",
                    defaultAvroFileLocator.listNewRecords().findFirst().isPresent());
        assertEquals("Wrong number of new events logged",
                     1, explicitAvroFileLocator.listNewRecords().count());
    }

    @Test
    public void shouldSupportMultipleSinks() throws IOException, InterruptedException {
        // Test that multiple hdfs sinks are supported for a single mapping.
        final AvroFileLocator defaultAvroFileLocator = new AvroFileLocator(Paths.get("/tmp"));
        final Path avroDirectory1 = createTempDirectory();
        final Path avroDirectory2 = createTempDirectory();
        startServer("hdfs-sink-multiple.conf", ImmutableMap.of(
                "divolte.sinks.test-hdfs-sink-1.file_strategy.working_dir", avroDirectory1.toString(),
                "divolte.sinks.test-hdfs-sink-1.file_strategy.publish_dir", avroDirectory1.toString(),
                "divolte.sinks.test-hdfs-sink-2.file_strategy.working_dir", avroDirectory2.toString(),
                "divolte.sinks.test-hdfs-sink-2.file_strategy.publish_dir", avroDirectory2.toString()
        ));
        final AvroFileLocator explicitAvroFileLocator1 = new AvroFileLocator(avroDirectory1);
        final AvroFileLocator explicitAvroFileLocator2 = new AvroFileLocator(avroDirectory2);
        request();
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - The default location (/tmp) shouldn't have anything new.
        //   - Our locations should both have a single record.
        assertFalse("Default location (/tmp) shouldn't have any new logged events.",
                    defaultAvroFileLocator.listNewRecords().findFirst().isPresent());
        assertEquals("Wrong number of new events logged in first location",
                     1, explicitAvroFileLocator1.listNewRecords().count());
        assertEquals("Wrong number of new events logged in second location",
                     1, explicitAvroFileLocator2.listNewRecords().count());
    }

    @Test
    public void shouldSupportMultipleMappings() throws IOException, InterruptedException {
        // Test that multiple independent mappings are supported.
        final Path avroDirectory1 = createTempDirectory();
        final Path avroDirectory2 = createTempDirectory();
        startServer("mapping-configuration-independent.conf", ImmutableMap.of(
                "divolte.sinks.sink-1.file_strategy.working_dir", avroDirectory1.toString(),
                "divolte.sinks.sink-1.file_strategy.publish_dir", avroDirectory1.toString(),
                "divolte.sinks.sink-2.file_strategy.working_dir", avroDirectory2.toString(),
                "divolte.sinks.sink-2.file_strategy.publish_dir", avroDirectory2.toString()
        ));
        final AvroFileLocator explicitAvroFileLocator1 = new AvroFileLocator(avroDirectory1);
        final AvroFileLocator explicitAvroFileLocator2 = new AvroFileLocator(avroDirectory2);
        request("/source-1");
        request("/source-2");
        request("/source-2");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - One source should have a single event.
        //   - The other should have a two events.
        assertEquals("Wrong number of new events logged in first location",
                     1, explicitAvroFileLocator1.listNewRecords().count());
        assertEquals("Wrong number of new events logged in second location",
                     2, explicitAvroFileLocator2.listNewRecords().count());
    }

    @Test
    public void shouldSupportMultipleMappingsPerSource() throws IOException, InterruptedException {
        // Test that a single source can send events to multiple mappings.
        final Path avroDirectory1 = createTempDirectory();
        final Path avroDirectory2 = createTempDirectory();
        startServer("mapping-configuration-shared-source.conf", ImmutableMap.of(
                "divolte.sinks.sink-1.file_strategy.working_dir", avroDirectory1.toString(),
                "divolte.sinks.sink-1.file_strategy.publish_dir", avroDirectory1.toString(),
                "divolte.sinks.sink-2.file_strategy.working_dir", avroDirectory2.toString(),
                "divolte.sinks.sink-2.file_strategy.publish_dir", avroDirectory2.toString()
        ));
        final AvroFileLocator explicitAvroFileLocator1 = new AvroFileLocator(avroDirectory1);
        final AvroFileLocator explicitAvroFileLocator2 = new AvroFileLocator(avroDirectory2);
        request();
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - Both sinks should have a single event.
        assertEquals("Wrong number of new events logged in first location",
                     1, explicitAvroFileLocator1.listNewRecords().count());
        assertEquals("Wrong number of new events logged in second location",
                     1, explicitAvroFileLocator2.listNewRecords().count());
    }

    @Test
    public void shouldSupportMultipleMappingsPerSink() throws IOException, InterruptedException {
        // Test that a multiple mappings can send events to the same sink.
        final Path avroDirectory = createTempDirectory();
        startServer("mapping-configuration-shared-sink.conf", ImmutableMap.of(
                "divolte.sinks.only-sink.file_strategy.working_dir", avroDirectory.toString(),
                "divolte.sinks.only-sink.file_strategy.publish_dir", avroDirectory.toString()
        ));
        final AvroFileLocator explicitAvroFileLocator = new AvroFileLocator(avroDirectory);
        request("/source-1");
        request("/source-2");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - The single location should have received both events.
        assertEquals("Wrong number of new events logged",
                     2, explicitAvroFileLocator.listNewRecords().count());
    }

    @Test
    public void shouldSupportComplexSourceMappingSinkConfigurations() throws IOException, InterruptedException {
        // Test that a complex source-mapping-sink configuration is possible.
        // (This includes combinations of shared and non-shared sources and sinks.)
        // Test that a single source can send events to multiple mappings.
        final Path avroDirectory1 = createTempDirectory();
        final Path avroDirectory2 = createTempDirectory();
        final Path avroDirectory3 = createTempDirectory();
        final Path avroDirectory4 = createTempDirectory();
        startServer("mapping-configuration-interdependent.conf", new ImmutableMap.Builder<String,Object>()
                .put("divolte.sinks.sink-1.file_strategy.working_dir", avroDirectory1.toString())
                .put("divolte.sinks.sink-1.file_strategy.publish_dir", avroDirectory1.toString())
                .put("divolte.sinks.sink-2.file_strategy.working_dir", avroDirectory2.toString())
                .put("divolte.sinks.sink-2.file_strategy.publish_dir", avroDirectory2.toString())
                .put("divolte.sinks.sink-3.file_strategy.working_dir", avroDirectory3.toString())
                .put("divolte.sinks.sink-3.file_strategy.publish_dir", avroDirectory3.toString())
                .put("divolte.sinks.sink-4.file_strategy.working_dir", avroDirectory4.toString())
                .put("divolte.sinks.sink-4.file_strategy.publish_dir", avroDirectory4.toString())
                .build()
        );
        final AvroFileLocator explicitAvroFileLocator1 = new AvroFileLocator(avroDirectory1);
        final AvroFileLocator explicitAvroFileLocator2 = new AvroFileLocator(avroDirectory2);
        final AvroFileLocator explicitAvroFileLocator3 = new AvroFileLocator(avroDirectory3);
        final AvroFileLocator explicitAvroFileLocator4 = new AvroFileLocator(avroDirectory4);
        request("/source-1");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        request("/source-2");
        testServer.get().waitForEvent();
        testServer.get().waitForEvent();
        request("/source-3");
        testServer.get().waitForEvent();
        request("/source-4");
        testServer.get().waitForEvent();
        // Stopping the server flushes any HDFS files.
        stopServer();
        // Now we can check:
        //   - Each sink should have a specific number of events in it.
        assertEquals("Wrong number of new events logged in first location",
                     2, explicitAvroFileLocator1.listNewRecords().count());
        assertEquals("Wrong number of new events logged in second location",
                     2, explicitAvroFileLocator2.listNewRecords().count());
        assertEquals("Wrong number of new events logged in third location",
                     5, explicitAvroFileLocator3.listNewRecords().count());
        assertEquals("Wrong number of new events logged in fourth location",
                     2, explicitAvroFileLocator4.listNewRecords().count());
    }

    @After
    public void tearDown() throws IOException {
        stopServer();
        cleanupTempDirectories();
    }

    private static void deleteRecursively(final Path p) {
        try (final Stream<Path> files = Files.walk(p).sorted(Comparator.reverseOrder())) {
            files.forEachOrdered(path -> {
                try {
                    Files.delete(path);
                } catch (final IOException e) {
                    throw new UncheckedIOException("Error deleting file: " + path, e);
                }
            });
        } catch (final IOException e) {
            throw new UncheckedIOException("Error recursively deleting directory: " + p, e);
        }
    }
}
