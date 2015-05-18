package io.divolte.server.recordmapping;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class SchemaMappingException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SchemaMappingException(String message) {
        super(message);
    }

    public SchemaMappingException(String message, Object... args) {
        this(String.format(message, args));
    }
}
