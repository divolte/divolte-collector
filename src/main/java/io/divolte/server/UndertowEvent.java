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

import io.undertow.server.HttpServerExchange;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;
import java.util.Objects;

@ParametersAreNonnullByDefault
public abstract class UndertowEvent {
    public final Instant requestTime;
    public final HttpServerExchange exchange;
    public final DivolteIdentifier partyId;

    public UndertowEvent(final Instant requestTime, final HttpServerExchange exchange, final DivolteIdentifier partyId) {
        this.requestTime = Objects.requireNonNull(requestTime);
        this.exchange = Objects.requireNonNull(exchange);
        this.partyId = Objects.requireNonNull(partyId);
    }

    public abstract DivolteEvent parseRequest() throws IncompleteRequestException;
}
