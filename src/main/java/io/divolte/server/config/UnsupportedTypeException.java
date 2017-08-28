package io.divolte.server.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class UnsupportedTypeException extends MismatchedInputException {
    private static final long serialVersionUID = 1L;

    public final JsonToken token;

    protected UnsupportedTypeException(final JsonParser p,
                                       final JsonToken token,
                                       final String msg) {
        super(Objects.requireNonNull(p),
              Objects.requireNonNull(msg));
        this.token = Objects.requireNonNull(token);
    }



    public static UnsupportedTypeException from(final JsonParser p,
                                                final JsonToken token,
                                                final String msg) {
        return new UnsupportedTypeException(p, token, msg);
    }
}
