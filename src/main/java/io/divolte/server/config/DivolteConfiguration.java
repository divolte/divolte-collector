package io.divolte.server.config;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class DivolteConfiguration {
    public final ServerConfiguration server;
    public final TrackingConfiguration tracking;
    public final JavascriptConfiguration javascript;
    public final IncomingRequestProcessorConfiguration incomingRequestProcessor;
    public final KafkaFlusherConfiguration kafkaFlusher;
    public final HdfsFlusherConfiguration hdfsFlusher;

    @JsonCreator
    private DivolteConfiguration(
            final ServerConfiguration server,
            final TrackingConfiguration tracking,
            final JavascriptConfiguration javascript,
            final IncomingRequestProcessorConfiguration incomingRequestProcessor,
            final KafkaFlusherConfiguration kafkaFlusher,
            final HdfsFlusherConfiguration hdfsFlusher) {
        this.server = server;
        this.tracking = tracking;
        this.javascript = javascript;
        this.incomingRequestProcessor = incomingRequestProcessor;
        this.kafkaFlusher = kafkaFlusher;
        this.hdfsFlusher = hdfsFlusher;
    }

    @Override
    public String toString() {
        return "DivolteConfiguration [server=" + server + ", tracking=" + tracking + ", javascript=" + javascript + ", incomingRequestProcessor=" + incomingRequestProcessor + ", kafkaFlusher=" + kafkaFlusher + ", hdfsFlusher=" + hdfsFlusher + "]";
    }
}
