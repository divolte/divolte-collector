package io.divolte.server.filesinks.gcs.entities;

import com.google.common.collect.ImmutableList;

public class ComposeRequest {
    public final ImmutableList<SourceObject> sourceObjects;

    public ComposeRequest(final ImmutableList<SourceObject> sourceObjects) {
        super();
        this.sourceObjects = sourceObjects;
    }

    public static class SourceObject {
        public final String name;

        public SourceObject(final String name) {
            this.name = name;
        }
    }
}
