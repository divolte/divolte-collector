package io.divolte.server.filesinks.gcs.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

/*
 * Note that these fields are not exhaustive for the get bucket response.
 * Just enough to display a nice enough log message.
 */
public final class GetBucketResponse {
    public final String id;
    public final String location;
    public final String name;
    public final String storageClass;
    public final String timeCreated;

    @JsonCreator
    public GetBucketResponse(final String id, final String location, final String name, final String storageClass, final String timeCreated) {
        super();
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
