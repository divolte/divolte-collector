package io.divolte.server.topicsinks.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.DivolteIdentifier;
import io.divolte.server.DivolteSchema;
import io.divolte.server.processing.Item;
import io.divolte.server.processing.ItemProcessor;
import io.grpc.Status;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@ParametersAreNonnullByDefault
public class GoogleCloudPubSubFlusherTest {
    private static final Schema MINIMAL_SCHEMA =
        SchemaBuilder.record("test")
                     .fields().requiredString("partyId")
                              .requiredString("sessionId")
                              .requiredLong("counter")
                     .endRecord();

    private Optional<DivolteIdentifier> partyId = Optional.empty();
    private Optional<DivolteIdentifier> sessionId = Optional.empty();
    private long generatedEventCounter;

    private Optional<Publisher> mockPublisher = Optional.empty();

    private long messageIdCounter;

    @Before
    public void resetMessages() {
        final long now = System.currentTimeMillis();
        partyId = Optional.of(DivolteIdentifier.generate(now));
        sessionId = Optional.of(DivolteIdentifier.generate(now));

        generatedEventCounter = 0;
        messageIdCounter = 0;

        final Publisher mockPublisher = mock(Publisher.class);
        when(mockPublisher.publish(any(PubsubMessage.class)))
            .thenAnswer(invocationOnMock -> completedFuture(String.valueOf(messageIdCounter++)));
        this.mockPublisher = Optional.of(mockPublisher);
    }

    @After
    public void clearIdentifiers() {
        partyId = Optional.empty();
        sessionId = Optional.empty();
        mockPublisher = Optional.empty();
    }

    private AvroRecordBuffer generateMessage() {
        final DivolteIdentifier partyId = this.partyId.orElseThrow(IllegalStateException::new);
        final DivolteIdentifier sessionId = this.sessionId.orElseThrow(IllegalStateException::new);
        final GenericRecord record = new GenericRecordBuilder(MINIMAL_SCHEMA)
            .set("partyId", partyId.toString())
            .set("sessionId", sessionId.toString())
            .set("counter", generatedEventCounter++)
            .build();
        return AvroRecordBuffer.fromRecord(partyId, sessionId, record);
    }

    private Item<AvroRecordBuffer> itemFromAvroRecordBuffer(final AvroRecordBuffer message) {
        return Item.of(0, message.getPartyId().value, message);
    }

    private static <V> ApiFuture<V> completedFuture(final V value) {
        final SettableApiFuture<V> future = SettableApiFuture.create();
        future.set(value);
        return future;
    }

    private static <V> ApiFuture<V> failedFuture(final Throwable throwable) {
        final SettableApiFuture<V> future = SettableApiFuture.create();
        future.setException(throwable);
        return future;
    }

    private void processSingleMessage() {
        processSingleMessage(Optional.empty());
    }

    private void processSingleMessage(final Optional<Integer> confluentId) {
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);

        // Process a single message.
        final DivolteSchema schema = new DivolteSchema(MINIMAL_SCHEMA, confluentId);
        final GoogleCloudPubSubFlusher flusher = new GoogleCloudPubSubFlusher(publisher, schema);
        if (ItemProcessor.ProcessingDirective.PAUSE == flusher.process(itemFromAvroRecordBuffer(generateMessage()))) {
            flusher.heartbeat();
        }
    }

    @Test
    public void testSingleMessageSentToPublisher() {
        // Process a single message.
        processSingleMessage();

        // Check it was forwarded to the publisher.
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);
        verify(publisher).publish(any(PubsubMessage.class));
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void testMessageBatchSentToPublisher() {
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);

        // Process a bunch of messages.
        final DivolteSchema schema = new DivolteSchema(MINIMAL_SCHEMA, Optional.empty());
        final GoogleCloudPubSubFlusher flusher = new GoogleCloudPubSubFlusher(publisher, schema);
        final Queue<Item<AvroRecordBuffer>> items =
            Stream.generate(this::generateMessage)
                  .limit(10)
                  .map(this::itemFromAvroRecordBuffer)
                  .collect(Collectors.toCollection(() -> new ArrayBlockingQueue<>(10)));
        flusher.process(items);

        // Check the messages were all forwarded to the publisher.
        verify(publisher, times(10)).publish(any(PubsubMessage.class));
        verifyNoMoreInteractions(publisher);
    }

    private PubsubMessage getFirstPublishedMessage() {
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);
        final ArgumentCaptor<PubsubMessage> argumentCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        verify(publisher).publish(argumentCaptor.capture());
        return argumentCaptor.getValue();
    }

    @Test
    public void testMessageBodyIsNakedAvroRecord() throws IOException {
        processSingleMessage();
        final PubsubMessage deliveredMessage = getFirstPublishedMessage();
        final ByteString body = deliveredMessage.getData();

        final DatumReader<GenericRecord> reader = new GenericDatumReader<>(MINIMAL_SCHEMA);
        final Decoder decoder = DecoderFactory.get().binaryDecoder(body.newInput(), null);
        final GenericRecord record = reader.read(null, decoder);
        assertEquals(partyId.orElseThrow(IllegalStateException::new).toString(), record.get("partyId").toString());
        assertEquals(sessionId.orElseThrow(IllegalStateException::new).toString(), record.get("sessionId").toString());
        assertEquals(0L, record.get("counter"));
    }

    @Test
    public void testMessagesHavePartyIdAttribute() {
        processSingleMessage();
        final PubsubMessage deliveredMessage = getFirstPublishedMessage();
        assertEquals(partyId.orElseThrow(IllegalStateException::new).toString(),
                     deliveredMessage.getAttributesOrThrow("partyIdentifier"));
    }

    @Test
    public void testMessagesHaveSchemaFingerprint() {
        processSingleMessage();
        // Reminder: fingerprint is the SHA-256 hash of the normalized schema,
        //           base-64 encoded using the URL-safe encoding,
        //           with trailing padding stripped.
        final String expectedFingerPrint =
            BaseEncoding.base64Url()
                        .encode(Hashing.sha256()
                                       .hashString(SchemaNormalization.toParsingForm(MINIMAL_SCHEMA),
                                                   StandardCharsets.UTF_8)
                                       .asBytes())
                        .replace("=", "");
        final PubsubMessage deliveredMessage = getFirstPublishedMessage();
        assertEquals(expectedFingerPrint, deliveredMessage.getAttributesOrThrow("schemaFingerprint"));
    }

    @Test
    public void testMessagesHaveConfluentIdIfKnown() {
        processSingleMessage(Optional.of(0x252));
        final PubsubMessage deliveredMessage = getFirstPublishedMessage();
        assertEquals("0x252", deliveredMessage.getAttributesOrThrow("schemaConfluentId"));
    }

    @Test
    public void testMessagesAreRetriedOnRetriableFailure() throws IOException {
        // Simulate a failure on the first send that indicates a retry should succeed.
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);
        when(publisher.publish(any(PubsubMessage.class)))
            .thenReturn(failedFuture(new ApiException("simulated transient failure",
                                                      new IOException(),
                                                      GrpcStatusCode.of(Status.Code.INTERNAL),
                                                      true)))
            .thenAnswer(invocationOnMock -> completedFuture(String.valueOf(messageIdCounter++)));

        // Here we send the message.
        processSingleMessage();

        // Now we check the invocations…
        verify(publisher, times(2)).publish(any(PubsubMessage.class));
        verifyNoMoreInteractions(publisher);
    }

    @Test
    public void testMessagesAreAbandonedOnNonRetriableFailure() throws IOException {
        // Simulate a failure on send that indicates a retry isn't allowed.
        final Publisher publisher = mockPublisher.orElseThrow(IllegalStateException::new);
        when(publisher.publish(any(PubsubMessage.class)))
            .thenReturn(failedFuture(new ApiException("simulated permanent failure",
                                                      new IOException(),
                                                      GrpcStatusCode.of(Status.Code.NOT_FOUND),
                                                      false)));
        // Here we send the message.
        processSingleMessage();

        // Now we check the invocations…
        verify(publisher).publish(any(PubsubMessage.class));
        verifyNoMoreInteractions(publisher);
    }
}
