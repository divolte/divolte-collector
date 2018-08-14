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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@JsonDeserialize(using = GlobalConfiguration.CustomDeserializer.class)
@ParametersAreNonnullByDefault
public class GlobalConfiguration {
    @Valid
    public final ServerConfiguration server;

    @Valid
    public final MapperConfiguration mapper;

    @Valid
    private final ImmutableMap<String, ? extends SinkTypeConfiguration> sinkTypeConfigurations;

    private final ImmutableMap<Class<? extends SinkTypeConfiguration>, ? extends SinkTypeConfiguration> sinkTypeConfigurationsByClass;


    GlobalConfiguration(final Optional<ServerConfiguration> server,
                        final Optional<MapperConfiguration> mapper,
                        final ImmutableMap<String, ? extends SinkTypeConfiguration> sinkTypeConfigurations) {
        this.server = server.orElseThrow(NullPointerException::new);
        this.mapper = mapper.orElseThrow(NullPointerException::new);
        this.sinkTypeConfigurations = sinkTypeConfigurations;

        sinkTypeConfigurationsByClass =
            sinkTypeConfigurations.values()
                .stream()
                .collect(ImmutableMap.toImmutableMap(SinkTypeConfiguration::getClass, Function.identity()));
    }

    public <T extends SinkTypeConfiguration> T getSinkTypeConfiguration(final Class<T> configurationClass) {
        return Optional.ofNullable(sinkTypeConfigurationsByClass.get(configurationClass))
            .map(configurationClass::cast)
            .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public String toString() {
        final MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this)
            .add("server", server)
            .add("mapper", mapper);
        sinkTypeConfigurations.forEach(stringHelper::add);
        return stringHelper.toString();
    }

    /**
     * This custom deserializer allows dynamic loading of sink configuration attributes in the global configuration. For
     * unknown properties, try to load a class from io.divolte.server.config.****.Configuration which should be a
     * subtype of {@link SinkTypeConfiguration}.
     */
    public static class CustomDeserializer extends StdDeserializer<GlobalConfiguration> {

        protected CustomDeserializer() {
            super(GlobalConfiguration.class);
        }

        @Override
        public GlobalConfiguration deserialize(JsonParser jp, DeserializationContext cntx) throws IOException, JsonProcessingException {
            final ObjectMapper mapper = (ObjectMapper) jp.getCodec();
            final JsonNode node = mapper.readTree(jp);

            Optional<ServerConfiguration> serverConfiguration = Optional.empty();
            Optional<MapperConfiguration> mapperConfiguration = Optional.empty();
            final ImmutableMap.Builder<String, SinkTypeConfiguration> builder = ImmutableMap.builder();

            final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> n = fields.next();

                String key = n.getKey();
                switch (key) {
                    case "server":
                        serverConfiguration = Optional.of(mapper.convertValue(n.getValue(), ServerConfiguration.class));
                        break;
                    case "mapper":
                        mapperConfiguration = Optional.of(mapper.convertValue(n.getValue(), MapperConfiguration.class));
                        break;
                    case "hdfs":
                        builder.put(key, mapper.convertValue(n.getValue(), HdfsConfiguration.class));
                        break;
                    case "kafka":
                        builder.put(key, mapper.convertValue(n.getValue(), KafkaConfiguration.class));
                        break;
                    case "gcs":
                        builder.put(key, mapper.convertValue(n.getValue(), GoogleCloudStorageConfiguration.class));
                        break;
                    case "gcps":
                        builder.put(key, mapper.convertValue(n.getValue(), GoogleCloudPubSubConfiguration.class));
                        break;
                    default:
                        final String otherClassName = String.format("io.divolte.server.config.%s.Configuration", key);
                        try {
                            final Class<?> candicate = Class.forName(otherClassName);
                            Class<? extends SinkTypeConfiguration> subclass = candicate.asSubclass(SinkTypeConfiguration.class);
                            builder.put(key, mapper.convertValue(n.getValue(), subclass));
                        } catch (final ClassCastException e) {
                            throw new IllegalStateException("Invalid configuration class for type " + key, e);
                        } catch (final ClassNotFoundException e) {
                            throw new IllegalStateException("Could not locate configuration class for type " + key, e);
                        }
                }
            }
            return new GlobalConfiguration(serverConfiguration, mapperConfiguration, builder.build());
        }
    }
}
