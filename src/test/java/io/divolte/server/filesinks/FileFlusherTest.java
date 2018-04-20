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

package io.divolte.server.filesinks;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.mockito.InOrder;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.config.FileStrategyConfiguration;
import io.divolte.server.config.HdfsSinkConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.filesinks.FileManager.DivolteFile;
import io.divolte.server.processing.Item;

public class FileFlusherTest {
    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    private static final String ARBITRARY_IP = "8.8.8.8";
    private final Schema schema;

    public FileFlusherTest() throws IOException {
        schema = schemaFromClassPath("/MinimalRecord.avsc");
    }

    @Test
    public void shouldSyncAndRollFile() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("200 milliseconds", "1 hour", "2");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);


        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        assertEquals(CONTINUE, flusher.process(item));
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file, times(2)).append(item.payload);
        calls.verify(file).sync();

        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file).append(item.payload);

        Thread.sleep(300);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(file).closeAndPublish();
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldSyncAndRollFileTimeBased() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("300 milliseconds", "50 milliseconds", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);


        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file).append(item.payload);

        Thread.sleep(100);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(file).sync();

        Thread.sleep(400);
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file).append(item.payload);
        calls.verify(file).closeAndPublish();
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRollFileOnHeartbeatWithNoPendingRecords() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("100 milliseconds", "1 hour", "1");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);


        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file).append(item.payload);

        Thread.sleep(200);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(file).closeAndPublish();
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldDiscardEmptyFile() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("100 milliseconds", "1 hour", "1");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);


        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        Thread.sleep(200);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(file).discard();
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldCloseAndPublishOnExitBeforeSync() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);


        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        assertEquals(CONTINUE, flusher.process(item));
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file, times(2)).append(item.payload);

        assertEquals(CONTINUE, flusher.heartbeat());

        flusher.cleanup();

        calls.verify(file).closeAndPublish();

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldDiscardEmptyFileOnExit() throws IOException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        assertEquals(CONTINUE, flusher.heartbeat());

        flusher.cleanup();

        calls.verify(file).discard();

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPauseAndAttemptDiscardOnAnyFailure() throws IOException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 1L);

        // throw exception on first record
        doThrow(new IOException("append")).when(file).append(item.payload);

        assertEquals(PAUSE, flusher.process(item));

        calls.verify(file).append(item.payload);
        calls.verify(file).discard();

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAttemptReconnectAfterProcessFailure() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        // Expect new file creation on file flusher construction
        when(manager.createFile(anyString())).thenReturn(file);
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);

        // throw exception on first record
        doThrow(new IOException("append")).when(file).append(item.payload);

        assertEquals(PAUSE, flusher.process(item));

        calls.verify(file).append(item.payload);
        calls.verify(file).discard();

        Thread.sleep(100);

        // Recover on first attempt, since creating a new file works
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAttemptReconnectMoreThanOnceAfterProcessFailure() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        when(manager.createFile(anyString()))
            .thenReturn(file)                            // Flusher construction succeeds
            .thenThrow(new IOException("create file"))   // Second file creation fails
            .thenReturn(file);                           // Third creation succeeds

        doThrow(new IOException("append"))
            .when(file).append(item.payload);            // first append fails

        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);

        assertEquals(PAUSE, flusher.process(item));

        calls.verify(file).append(item.payload);
        calls.verify(file).discard();

        Thread.sleep(100);

        // Fail recovery on first attempt
        assertEquals(PAUSE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString());

        Thread.sleep(100);

        // Succeed recovery on second attempt
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAttemptReconnectAfterHeartbeatSyncFailure() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "50 milliseconds", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        when(manager.createFile(anyString()))
            .thenReturn(file)                            // Flusher construction succeeds
            .thenReturn(file);                           // Second file creation succeeds

        doThrow(new IOException("sync")).when(file).sync();

        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);

        assertEquals(CONTINUE, flusher.process(item));
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file, times(2)).append(item.payload);

        Thread.sleep(100);
        assertEquals(PAUSE, flusher.heartbeat()); // Sync fails at this point
        calls.verify(file).sync();
        calls.verify(file).discard();
        // No attempt at rolling / creating a new file should be made at this point

        Thread.sleep(100);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString()); // Reconnect attempt

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAttemptReconnectAfterHeartbeatRollCloseFailure() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("50 milliseconds", "1 hour", "2");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        when(manager.createFile(anyString()))
            .thenReturn(file)                            // Flusher construction succeeds
            .thenReturn(file);                           // Second file creation succeeds

        doThrow(new IOException("close")).when(file).closeAndPublish();

        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);

        assertEquals(CONTINUE, flusher.process(item));
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file, times(2)).append(item.payload);
        calls.verify(file).sync();

        Thread.sleep(100);
        assertEquals(PAUSE, flusher.heartbeat()); // Rolling the file fails at this point (due to closeAndPublish failure)
        calls.verify(file).closeAndPublish();
        calls.verify(file).discard();
        // No attempt at rolling / creating a new file should be made at this point

        Thread.sleep(100);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString()); // Reconnect attempt

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAttemptReconnectAfterHeartbeatRollCreateFailure() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("50 milliseconds", "1 hour", "2");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        when(manager.createFile(anyString()))
            .thenReturn(file)                            // Flusher construction succeeds
            .thenThrow(new IOException("file create"))   // Second file creation fails
            .thenReturn(file);                           // Third file creation succeeds

        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);

        assertEquals(CONTINUE, flusher.process(item));
        assertEquals(CONTINUE, flusher.process(item));
        calls.verify(file, times(2)).append(item.payload);
        calls.verify(file).sync();

        Thread.sleep(100);
        assertEquals(PAUSE, flusher.heartbeat()); // Rolling the file fails at this point (due to closeAndPublish failure)
        calls.verify(file).closeAndPublish();
        calls.verify(manager).createFile(anyString()); // File rolled; expecting creation of a new file, which fails

        Thread.sleep(100);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString()); // Reconnect attempt

        calls.verifyNoMoreInteractions();
    }

    @Test
    public void shouldPostponeFailureOnConstruction() throws IOException, InterruptedException {
        final FileStrategyConfiguration fileStrategyConfiguration = setupConfiguration("1 hour", "1 hour", "200");

        // Mocks
        final FileManager manager = mock(FileManager.class);
        final DivolteFile file = mock(DivolteFile.class);
        final InOrder calls = inOrder(manager, file);

        final Item<AvroRecordBuffer> item = itemFromAvroRecordBuffer(newAvroRecordBuffer());

        when(manager.createFile(anyString()))
            .thenThrow(new IOException("file create"))   // First file creation fails
            .thenReturn(file);                           // Second file creation fails

        // Actual failing invocation of manager.createNewFile(...) happens here
        final FileFlusher flusher = new FileFlusher(fileStrategyConfiguration, manager, 50L);
        calls.verify(manager).createFile(anyString());

        // Exception should be re-thrown at this point
        assertEquals(PAUSE, flusher.process(item));
        // Important: should not hit file.append(...)

        Thread.sleep(100);
        assertEquals(CONTINUE, flusher.heartbeat());
        calls.verify(manager).createFile(anyString());

        calls.verifyNoMoreInteractions();
    }

    private Item<AvroRecordBuffer> itemFromAvroRecordBuffer(final AvroRecordBuffer arb) {
        return Item.of(0, arb.getPartyId().value, arb);
    }

    private AvroRecordBuffer newAvroRecordBuffer() {
        final Instant now = Instant.now();
        final Record record = new GenericRecordBuilder(schema)
            .set("ts", now.toEpochMilli())
            .set("remoteHost", ARBITRARY_IP)
            .build();

        return AvroRecordBuffer.fromRecord(DivolteIdentifier.generate(),
                                           DivolteIdentifier.generate(),
                                           now,
                                           record);
    }

    private FileStrategyConfiguration setupConfiguration(final String roll, final String syncDuration, final String syncRecords) {
        final Config config = ConfigFactory
            .parseMap(ImmutableMap.<String, Object>builder()
                .put("divolte.sinks.hdfs.type", "hdfs")
                .put("divolte.sinks.hdfs.file_strategy.roll_every", roll)
                .put("divolte.sinks.hdfs.file_strategy.sync_file_after_duration", syncDuration)
                .put("divolte.sinks.hdfs.file_strategy.sync_file_after_records", syncRecords)
                .put("divolte.sinks.hdfs.file_strategy.working_dir", "/tmp/work")
                .put("divolte.sinks.hdfs.file_strategy.publish_dir", "/tmp/published")
                .build())
            .withFallback(ConfigFactory.parseResources("reference-test.conf"));
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> config);

        return vc.configuration().getSinkConfiguration("hdfs", HdfsSinkConfiguration.class).fileStrategy;
    }

    private Schema schemaFromClassPath(final String resource) throws IOException {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(resource)) {
            return new Schema.Parser().parse(resourceStream);
        }
    }

}
