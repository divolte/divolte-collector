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

package io.divolte.server.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Joiner;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Properties;

import static com.fasterxml.jackson.core.JsonToken.*;

@ParametersAreNonnullByDefault
public class PropertiesDeserializer extends JsonDeserializer<Properties> {
    private final static Joiner DOT_JOINER = Joiner.on('.');
    private final static Joiner COMMA_JOINER = Joiner.on(',');

    @Override
    public Properties deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        if (START_OBJECT == p.getCurrentToken()) {
            final Properties properties = new Properties();
            final Deque<String> stack = new ArrayDeque<>();
            final ArrayList<String> array = new ArrayList<>();
            for (JsonToken nextToken = p.nextToken(); nextToken != END_OBJECT || !stack.isEmpty(); nextToken = p.nextToken()) {
                switch(nextToken) {
                    case FIELD_NAME:
                        stack.addLast(p.getCurrentName());
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER_INT:
                    case VALUE_NUMBER_FLOAT:
                    case VALUE_TRUE:
                    case VALUE_FALSE:
                        if (p.getParsingContext().inArray()) {
                            array.add(p.getText());
                        } else {
                            properties.put(DOT_JOINER.join(stack), p.getText());
                            stack.removeLast();
                        }
                        break;
                    case START_OBJECT:
                        if (p.getParsingContext().inArray()) {
                            throw UnsupportedTypeException.from(p, START_OBJECT, "Nested objects within arrays not allowed in Properties object.");
                        }
                        break;
                    case END_OBJECT:
                        stack.removeLast();
                        break;
                    case START_ARRAY:
                        array.clear();
                        break;
                    case END_ARRAY:
                        properties.put(DOT_JOINER.join(stack), COMMA_JOINER.join(array));
                        stack.removeLast();
                        break;
                    case VALUE_NULL:
                        throw UnsupportedTypeException.from(p, VALUE_NULL, "Null values not allowed in Properties object.");
                    case VALUE_EMBEDDED_OBJECT:
                        throw UnsupportedTypeException.from(p, VALUE_EMBEDDED_OBJECT, "Embedded object not allowed as part of Properties object.");
                    case NOT_AVAILABLE:
                        break;
                }
            }
            return properties;
        } else {
            throw ctx.wrongTokenException(p, handledType(), START_OBJECT, "Expected nested object for Properties mapping.");
        }
    }
}
