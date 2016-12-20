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

package io.divolte.server.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Mock Schema Registy Client that does not actually retrieve schema IDs from the schema registry.
 */
class SingleSchemaRegistryClient implements SchemaRegistryClient {

    private final int schemaId;

    public SingleSchemaRegistryClient(int schemaId) {
        this.schemaId = schemaId;
    }

    @Override
    public int register(String s, Schema schema) throws IOException, RestClientException {
        return schemaId;
    }

    @Override
    public Schema getByID(int id) throws IOException, RestClientException {
        throw new IOException();
    }

    @Override
    public SchemaMetadata getLatestSchemaMetadata(String s) throws IOException, RestClientException {
        throw new IOException();
    }

    @Override
    public int getVersion(String s, Schema schema) throws IOException, RestClientException {
        throw new IOException();
    }

    @Override
    public boolean testCompatibility(String s, Schema schema) throws IOException, RestClientException {
        throw new IOException();
    }

    @Override
    public String updateCompatibility(String s, String s1) throws IOException, RestClientException {
        throw new IOException();
    }

    @Override
    public String getCompatibility(String s) throws IOException, RestClientException {
        throw new IOException();
    }
}
