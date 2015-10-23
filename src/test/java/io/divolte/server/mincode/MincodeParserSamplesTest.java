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

package io.divolte.server.mincode;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.format.DataFormatDetector;
import com.fasterxml.jackson.core.format.DataFormatMatcher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
@RunWith(Parameterized.class)
public class MincodeParserSamplesTest {
    private static final ObjectMapper JSON_MAPPER;
    static {
        JSON_MAPPER = new ObjectMapper();
        JSON_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> samples() throws IOException {
        // Load the samples from JSON.
        final URL samplesUrl = MincodeParserSamplesTest.class.getResource("mincode-samples.json");
        final JsonNode samples = JSON_MAPPER.readTree(samplesUrl);
        return StreamSupport.stream(samples.spliterator(), false)
                            .map(jsonNode -> new Object[]{
                                    jsonNode.path("title").asText("N/A"),
                                    Objects.requireNonNull(jsonNode.get("json"), "Missing JSON sample"),
                                    Objects.requireNonNull(jsonNode.get("code"), "Missing Mincode sample").asText()
                            })
                            .collect(Collectors.toList());
    }

    private final JsonNode sampleJson;
    private final String sampleMincode;

    private MincodeFactory factory;

    public MincodeParserSamplesTest(final String sampleTitle,
                                    final JsonNode sampleJson,
                                    final String sampleMincode) {
        this.sampleJson = Objects.requireNonNull(sampleJson);
        this.sampleMincode = Objects.requireNonNull(sampleMincode);
    }

    @Before
    public void setUp() throws Exception {
        factory = new MincodeFactory();
    }

    @After
    public void tearDown() throws Exception {
        factory = null;
    }

    @Test
    public void testMincodeFormatDetector() throws Exception {
        final DataFormatDetector detector = new DataFormatDetector(factory);
        final DataFormatMatcher format = detector.findFormat(this.sampleMincode.getBytes(StandardCharsets.UTF_8));
        assertEquals(factory.getFormatName(), format.getMatchedFormatName());
    }

    @Test
    public void testSampleEquivalence() throws IOException {
        // Test that samples, when parsed, generate the same sequence of parser states.
        final MincodeParser mincodeParser = factory.createParser(sampleMincode);
        final JsonParser jsonParser = sampleJson.traverse();

        // Initial state: parsers should have no current state.
        assertFalse(mincodeParser.hasCurrentToken());
        assertFalse(jsonParser.hasCurrentToken());

        for (;;) {
            final JsonToken jsonToken = jsonParser.nextToken();
            final JsonToken mincodeToken = mincodeParser.nextToken();
            assertEquals(jsonToken, mincodeToken);
            if (null == jsonToken) {
                // Both parsers are finished.
                break;
            }
            assertEquals(jsonToken, jsonParser.getCurrentToken());
            assertEquals(mincodeToken, mincodeParser.getCurrentToken());

            assertEquals(jsonParser.getCurrentName(), mincodeParser.getCurrentName());
            final Object currentJsonValue = getCurrentValue(jsonParser);
            final Object currentMincodeValue = getCurrentValue(mincodeParser);
            assertEquals(currentJsonValue, currentMincodeValue);
        }
    }

    private static Object getCurrentValue(final JsonParser parser) throws IOException {
        final Object value;
        switch (parser.getCurrentToken()) {
            case VALUE_STRING:
                value = parser.getText();
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
                value = parser.getBooleanValue();
                break;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                value = parser.getDecimalValue();
                break;
            case VALUE_NULL:
            default:
                value = null;
        }
        return value;
    }
}
