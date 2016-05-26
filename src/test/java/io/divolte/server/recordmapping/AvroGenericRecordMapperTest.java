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

package io.divolte.server.recordmapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@ParametersAreNonnullByDefault
@RunWith(Parameterized.class)
public class AvroGenericRecordMapperTest {
    private static final ObjectMapper JSON_MAPPER;
    static {
        JSON_MAPPER = new ObjectMapper();
        JSON_MAPPER.registerModules(new Jdk8Module(), new ParameterNamesModule());
        JSON_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_MAPPER.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        JSON_MAPPER.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true);
        JSON_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @JsonIgnoreProperties("title")
    private static class Fixture {
        public final Schema avroSchema;
        public final boolean schemaUnsupported;
        public final JsonNode jsonToMap;
        public final Map<DeserializationFeature,Boolean> deserializationFeatures;
        public final JsonNode expectedJson;
        public final Optional<Class<? extends Exception>> expectedException;

        @JsonCreator
        public Fixture(final JsonNode schema,
                       @JsonProperty("unsupported") final boolean schemaUnsupported,
                       @JsonProperty("json") final JsonNode jsonToMap,
                       final Optional<Map<DeserializationFeature,Boolean>> deserializationFeatures,
                       @JsonProperty("result") final JsonNode expectedJson,
                       @JsonProperty("exception") final Optional<Class<? extends Exception>> expectedException) throws JsonProcessingException {
            this.avroSchema = new Schema.Parser().parse(JSON_MAPPER.writeValueAsString(schema));
            this.schemaUnsupported = schemaUnsupported;
            this.jsonToMap = Objects.requireNonNull(jsonToMap);
            this.deserializationFeatures = deserializationFeatures.orElse(Collections.emptyMap());
            this.expectedJson = Objects.requireNonNull(expectedJson);
            this.expectedException = Objects.requireNonNull(expectedException);
        }
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> samples() throws IOException {
        // Load the samples from JSON.
        final URL samplesUrl = AvroGenericRecordMapperTest.class.getResource("jackson-mapping-fixtures.json");
        final JsonNode samples = JSON_MAPPER.readTree(samplesUrl);
        return StreamSupport.stream(samples.spliterator(), false)
                            .map(jsonNode -> new Object[]{
                                    jsonNode.path("title").asText("N/A"),
                                    jsonToFixture(jsonNode)
                            })
                            .collect(Collectors.toList());
    }

    private static Fixture jsonToFixture(final JsonNode jsonNode)  {
        try {
            return JSON_MAPPER.treeToValue(jsonNode, Fixture.class);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot convert JSON to fixture: " + jsonNode, e);
        }
    }

    private final Fixture testFixture;

    private AvroGenericRecordMapper reader;

    public AvroGenericRecordMapperTest(final String sampleTitle,
                                       final Fixture testFixture) {
        this.testFixture = Objects.requireNonNull(testFixture);
    }

    @Before
    public void setUp() throws Exception {
        ObjectReader objectReader = JSON_MAPPER.reader();
        for (final Map.Entry<DeserializationFeature,Boolean> entry : testFixture.deserializationFeatures.entrySet()) {
            final DeserializationFeature feature = entry.getKey();
            objectReader = entry.getValue()
                    ? objectReader.with(feature)
                    : objectReader.without(feature);
        }
        reader = new AvroGenericRecordMapper(objectReader);
    }

    @After
    public void tearDown() throws Exception {
        reader = null;
    }

    @Test
    public void testValidation() {
        /*
         * Test what happens when schemas are validated.
         */
        final Optional<ValidationError> validationError = reader.checkValid(testFixture.avroSchema);
        assertEquals(testFixture.schemaUnsupported, validationError.isPresent());
    }

    @Test
    public void testMapping() throws Exception {
        /*
         * Test what happens when JSON samples are mapped to a specific Avro schema.
         *
         * The outcome can be either:
         *  - An expected JSON result (defaults to the input JSON)
         *  - An expected exception occurs.
         *
         * A fixture can also specify specific deserialization options be [in]active.
         */
        try {
            final Object avroResult = reader.read(testFixture.jsonToMap, testFixture.avroSchema);
            // If we expected an exception, fail...
            testFixture.expectedException.ifPresent(e -> fail("Expected exception to be thrown: " + e));
            // ...otherwise verify the result.
            final JsonNode avroResultJson = JSON_MAPPER.readTree(GenericData.get().toString(avroResult));
            assertEquals(testFixture.expectedJson, avroResultJson);
        } catch (final Exception e) {
            // Suppress the exception if it was expected; otherwise rethrow.
            testFixture.expectedException
                    .filter(ee -> ee.isInstance(e))
                    .orElseThrow(() -> e);
        }
    }
}
