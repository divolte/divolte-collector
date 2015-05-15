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
    // Events from all sources support these attributes.
    public final boolean corruptEvent;
    public final DivolteIdentifier partyCookie;
    public final DivolteIdentifier sessionCookie;
    public final String eventId;
    public final String eventSource;
    public final Optional<String> eventType;

    public final boolean newPartyId;
    public final boolean firstInSession;

    public final long requestStartTime;
    public final long clientUtcOffset;

    public final Supplier<Optional<JsonNode>> eventParametersProducer;
    // Extra data provided for browser events.
    public final Optional<BrowserEventData> browserEventData;

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

    DivolteEvent(final boolean corruptEvent,
                 final DivolteIdentifier partyCookie,
                 final DivolteIdentifier sessionCookie,
                 final String eventId,
                 final String eventSource,
                 final long requestStartTime,
                 final long clientUtcOffset,
                 final boolean newPartyId,
                 final boolean firstInSession,
                 final Optional<String> eventType,
                 final Supplier<Optional<JsonNode>> eventParametersProducer,
                 final Optional<BrowserEventData> browserEvent) {
        this.corruptEvent            = corruptEvent;
        this.partyCookie             = Objects.requireNonNull(partyCookie);
        this.sessionCookie           = Objects.requireNonNull(sessionCookie);
        this.eventId                 = Objects.requireNonNull(eventId);
        this.eventSource             = Objects.requireNonNull(eventSource);
        this.requestStartTime        = requestStartTime;
        this.clientUtcOffset         = clientUtcOffset;
        this.newPartyId              = newPartyId;
        this.firstInSession          = firstInSession;
        this.eventType               = Objects.requireNonNull(eventType);
        this.eventParametersProducer = Objects.requireNonNull(eventParametersProducer);
        this.browserEventData        = Objects.requireNonNull(browserEvent);
    }
}
