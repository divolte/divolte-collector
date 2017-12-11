/*
 * Copyright 2017 GoDataDriven B.V.
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

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.ParametersAreNonnullByDefault;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;

import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.SchemaRegistry;
import io.divolte.server.processing.ProcessingPool;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=HdfsSinkConfiguration.class, name = "hdfs"),
        @JsonSubTypes.Type(value=GoogleCloudStorageSinkConfiguration.class, name = "gcs"),
        @JsonSubTypes.Type(value=KafkaSinkConfiguration.class, name = "kafka"),
        @JsonSubTypes.Type(value=GoogleCloudPubSubSinkConfiguration.class, name = "gcps"),
})
@ParametersAreNonnullByDefault
public abstract class SinkConfiguration {
    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public final String toString() {
        return toStringHelper().toString();
    }

    @JsonIgnore
    public abstract SinkFactory getFactory();

    @FunctionalInterface
    public interface SinkFactory {
        ProcessingPool<?, AvroRecordBuffer> create(ValidatedConfiguration configuration,
                                                   String sinkName,
                                                   SchemaRegistry schemaRegistry);
    }
}
