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

package io.divolte.server;

import com.google.common.base.MoreObjects;
import org.apache.avro.Schema;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class DivolteSchema {

    public final Schema avroSchema;
    public final Optional<Integer> confluentId;

    public DivolteSchema(final Schema avroSchema, final Optional<Integer> confluentId) {
        this.avroSchema = Objects.requireNonNull(avroSchema);
        this.confluentId = Objects.requireNonNull(confluentId);
    }

    @Override
    public boolean equals(final Object other) {
        return this == other
            || other instanceof DivolteSchema && equals((DivolteSchema) other);
    }

    public boolean equals(final DivolteSchema other) {
        return this == other
            || Objects.equals(avroSchema, other.avroSchema)
               && Objects.equals(confluentId, other.confluentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(avroSchema, confluentId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("avroSchema", avroSchema)
            .add("confluentId", confluentId)
            .toString();
    }
}
