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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;

import io.divolte.server.HttpSource;
import io.divolte.server.IncomingRequestProcessingPool;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value=BrowserSourceConfiguration.class, name = "browser"),
        @JsonSubTypes.Type(value=JsonSourceConfiguration.class, name = "json"),
})
@ParametersAreNonnullByDefault
public abstract class SourceConfiguration {
    @OverridingMethodsMustInvokeSuper
    protected MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }

    public abstract HttpSource createSource(
            ValidatedConfiguration configuration,
            String sourceName,
            IncomingRequestProcessingPool processingPool);
}
