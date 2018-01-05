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

package io.divolte.server.filesinks.gcs.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNullableByDefault;

/*
 * Note that these fields are not exhaustive for the get bucket response.
 * Just enough to display a nice enough log message.
 */
@ParametersAreNullableByDefault
public final class GetBucketResponse {
    public final String id;
    public final String location;
    public final String name;
    public final String storageClass;
    public final String timeCreated;

    @JsonCreator
    public GetBucketResponse(final String id, final String location, final String name, final String storageClass, final String timeCreated) {
        this.id = id;
        this.location = location;
        this.name = name;
        this.storageClass = storageClass;
        this.timeCreated = timeCreated;
    }

    @Override
    public String toString() {
        return MoreObjects
            .toStringHelper("Bucket")
            .add("id", id)
            .add("name", name)
            .add("location", location)
            .add("storage class", storageClass)
            .add("created", timeCreated)
            .toString();
    }
}
