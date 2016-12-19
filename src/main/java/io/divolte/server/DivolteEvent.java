/*
 * Copyright 2015 GoDataDriven B.V.
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

import javax.annotation.ParametersAreNonnullByDefault;
import com.fasterxml.jackson.databind.JsonNode;

import io.undertow.server.HttpServerExchange;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Internal encapsulation of a Divolte event.
 *
 * All Divolte events support a minimum set of data, with
 * sources providing additional data.
 */
@ParametersAreNonnullByDefault
public final class DivolteEvent {
    public final HttpServerExchange exchange;

    // Events from all sources support these attributes.
    public final boolean corruptEvent;
    public final DivolteIdentifier partyId;
    public final DivolteIdentifier sessionId;
    public final String eventId;
    public final String eventSource;
    // This might be mildly surprising, but technically the event type is optional.
    // In practice it's normally filled in though.
    public final Optional<String> eventType;

    public final boolean newPartyId;
    public final boolean firstInSession;

    public final Instant requestStartTime;
    public final Instant clientTime;

    public final Supplier<Optional<JsonNode>> eventParametersProducer;

    // Data specific to browser events.
    public final Optional<BrowserEventData> browserEventData;

    // Data specific to JSON events
    public final Optional<JsonEventData> jsonEventData;

    @ParametersAreNonnullByDefault
    public static final class BrowserEventData {
        public final String pageViewId;
        public final Optional<String> location;
        public final Optional<String> referer;
        public final Optional<Integer> viewportPixelWidth;
        public final Optional<Integer> viewportPixelHeight;
        public final Optional<Integer> screenPixelWidth;
        public final Optional<Integer> screenPixelHeight;
        public final Optional<Integer> devicePixelRatio;

        BrowserEventData(final String pageViewId,
                         final Optional<String> location,
                         final Optional<String> referer,
                         final Optional<Integer> viewportPixelWidth,
                         final Optional<Integer> viewportPixelHeight,
                         final Optional<Integer> screenPixelWidth,
                         final Optional<Integer> screenPixelHeight,
                         final Optional<Integer> devicePixelRatio) {
            this.pageViewId          = Objects.requireNonNull(pageViewId);
            this.location            = Objects.requireNonNull(location);
            this.referer             = Objects.requireNonNull(referer);
            this.viewportPixelWidth  = Objects.requireNonNull(viewportPixelWidth);
            this.viewportPixelHeight = Objects.requireNonNull(viewportPixelHeight);
            this.screenPixelWidth    = Objects.requireNonNull(screenPixelWidth);
            this.screenPixelHeight   = Objects.requireNonNull(screenPixelHeight);
            this.devicePixelRatio    = Objects.requireNonNull(devicePixelRatio);
        }
    }

    @ParametersAreNonnullByDefault
    public static final class JsonEventData {
        /*
         * Empty for now. There's nothing specific to JSON events at the moment.
         *
         * Because of this, we can use a single instance throughout, just to
         * signal that a DivolteEvent is indeed a JSON source based event.
         */
        public static final JsonEventData EMPTY = new JsonEventData();

        public JsonEventData() {
        }
    }

    DivolteEvent(final HttpServerExchange originatingExchange,
                 final boolean corruptEvent,
                 final DivolteIdentifier partyCookie,
                 final DivolteIdentifier sessionCookie,
                 final String eventId,
                 final String eventSource,
                 final Instant requestStartTime,
                 final Instant clientTime,
                 final boolean newPartyId,
                 final boolean firstInSession,
                 final Optional<String> eventType,
                 final Supplier<Optional<JsonNode>> eventParametersProducer,
                 final Optional<BrowserEventData> browserEvent,
                 final Optional<JsonEventData> jsonEvent) {
        this.exchange                = originatingExchange;
        this.corruptEvent            = corruptEvent;
        this.partyId                 = Objects.requireNonNull(partyCookie);
        this.sessionId               = Objects.requireNonNull(sessionCookie);
        this.eventId                 = Objects.requireNonNull(eventId);
        this.eventSource             = Objects.requireNonNull(eventSource);
        this.requestStartTime        = Objects.requireNonNull(requestStartTime);
        this.clientTime              = Objects.requireNonNull(clientTime);
        this.newPartyId              = newPartyId;
        this.firstInSession          = firstInSession;
        this.eventType               = Objects.requireNonNull(eventType);
        this.eventParametersProducer = Objects.requireNonNull(eventParametersProducer);
        this.browserEventData        = Objects.requireNonNull(browserEvent);
        this.jsonEventData           = Objects.requireNonNull(jsonEvent);
    }

    static DivolteEvent createBrowserEvent(
            final HttpServerExchange originatingExchange,
            final boolean corruptEvent,
            final DivolteIdentifier partyCookie,
            final DivolteIdentifier sessionCookie,
            final String eventId,
            final Instant requestStartTime,
            final Instant clientUtcOffset,
            final boolean newPartyId,
            final boolean firstInSession,
            final Optional<String> eventType,
            final Supplier<Optional<JsonNode>> eventParametersProducer,
            final BrowserEventData browserEvent) {
        return new DivolteEvent(
                originatingExchange,
                corruptEvent,
                partyCookie,
                sessionCookie,
                eventId,
                BrowserSource.EVENT_SOURCE_NAME,
                requestStartTime,
                clientUtcOffset,
                newPartyId,
                firstInSession,
                eventType,
                eventParametersProducer,
                Optional.of(browserEvent),
                Optional.empty()
                );
    }

    static DivolteEvent createJsonEvent(
            final HttpServerExchange originatingExchange,
            final DivolteIdentifier partyCookie,
            final DivolteIdentifier sessionCookie,
            final String eventId,
            final String eventSource,
            final Instant requestStartTime,
            final Instant clientTime,
            final boolean newPartyId,
            final boolean firstInSession,
            final Optional<String> eventType,
            final Supplier<Optional<JsonNode>> eventParametersProducer,
            final JsonEventData jsonEvent) {
        return new DivolteEvent(
                originatingExchange,
                // Corruption in JSON events can't currently be detected.
                false,
                partyCookie,
                sessionCookie,
                eventId,
                eventSource,
                requestStartTime,
                clientTime,
                newPartyId,
                firstInSession,
                eventType,
                eventParametersProducer,
                Optional.empty(),
                Optional.of(jsonEvent)
                );
    }
}
