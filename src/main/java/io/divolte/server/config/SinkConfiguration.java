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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.google.common.base.MoreObjects;
import com.google.common.reflect.TypeToken;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.SchemaRegistry;
import io.divolte.server.processing.ProcessingPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@ParametersAreNonnullByDefault
@JsonTypeIdResolver(SinkConfiguration.SubclassResolver.class)
public abstract class SinkConfiguration<T extends SinkTypeConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(SinkTypeConfiguration.class);

    @SuppressWarnings("unchecked")
    private final Class<T> sinkTypeConfigurationClass = (Class<T>) new TypeToken<T>(getClass()) {}.getRawType();

    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }

    @JsonIgnore
    public abstract SinkFactory getFactory();

    @JsonIgnore
    public T getGlobalConfiguration(final ValidatedConfiguration configuration) {
        return configuration.configuration().global.getSinkTypeConfiguration(sinkTypeConfigurationClass);
    }

    @FunctionalInterface
    public interface SinkFactory {
        ProcessingPool<?, AvroRecordBuffer> create(ValidatedConfiguration configuration,
                                                   String sinkName,
                                                   SchemaRegistry schemaRegistry);
    }

    @ParametersAreNonnullByDefault
    static class SubclassResolver extends TypeIdResolverBase {
        private Optional<JavaType> superType = Optional.empty();

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return JsonTypeInfo.Id.NAME;
        }

        @Override
        public void init(final JavaType bt) {
            superType = Optional.ofNullable(bt);
        }

        @Override
        public String idFromValue(final Object value) {
            throw new IllegalStateException("Sink configurations are never serialized.");
        }

        @Override
        public String idFromValueAndType(final Object value, final Class<?> suggestedType) {
            throw new IllegalStateException("Sink configurations are never serialized.");
        }

        @Override
        public JavaType typeFromId(final DatabindContext context, final String id) {
            final Class<? extends SinkConfiguration> subclass;
            switch (id) {
                case "hdfs":
                    subclass = HdfsSinkConfiguration.class;
                    break;
                case "gcs":
                    subclass = GoogleCloudStorageSinkConfiguration.class;
                    break;
                case "kafka":
                    subclass = KafkaSinkConfiguration.class;
                    break;
                case "gcps":
                    subclass = GoogleCloudPubSubSinkConfiguration.class;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Unknown sink configuration type: %s", id));
            }
            return context.constructSpecializedType(superType.orElseThrow(IllegalStateException::new), subclass);
        }
    }
}
