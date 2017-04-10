package io.divolte.server.filesinks.gcs.entities;

import com.google.common.collect.ImmutableList;

public class ComposeRequest {
    public final ImmutableList<SourceObject> sourceObjects;
    public final DestinationObject destination;

    public ComposeRequest(final DestinationObject destination, final ImmutableList<SourceObject> sourceObjects) {
        this.destination = destination;
        this.sourceObjects = sourceObjects;
    }

    public static class SourceObject {
        public final String name;

        public SourceObject(final String name) {
            this.name = name;
        }
    }

    public static class DestinationObject {
        public final String contentType;

        public DestinationObject(final String contentType) {
            super();
            this.contentType = contentType;
        }
    }
}
