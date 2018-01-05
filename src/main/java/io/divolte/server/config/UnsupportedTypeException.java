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
