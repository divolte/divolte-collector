package io.divolte.server;

import org.apache.avro.generic.GenericRecord;

import io.undertow.server.HttpServerExchange;

interface IncomingRequestListener {
    void incomingRequest(HttpServerExchange exchange, AvroRecordBuffer avroBuffer, GenericRecord avroRecord);
}
