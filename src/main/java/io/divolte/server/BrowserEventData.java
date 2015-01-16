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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class BrowserEventData {
    public final boolean corruptEvent;
    public final CookieValues.CookieValue partyCookie;
    public final CookieValues.CookieValue sessionCookie;
    public final String pageViewId;
    public final String eventId;
    public final long requestStartTime;
    public final long clientUtcOffset;
    public final boolean newPartyId;
    public final boolean firstInSession;
    public final Optional<String> location;
    public final Optional<String> referer;
    public final Optional<String> eventType;
    public final Optional<Integer> viewportPixelWidth;
    public final Optional<Integer> viewportPixelHeight;
    public final Optional<Integer> screenPixelWidth;
    public final Optional<Integer> screenPixelHeight;
    public final Optional<Integer> devicePixelRatio;
    public final Function<String, Optional<String>> eventParameterProducer;

    BrowserEventData(final boolean corruptEvent,
                     final CookieValues.CookieValue partyCookie,
                     final CookieValues.CookieValue sessionCookie,
                     final String pageViewId,
                     final String eventId,
                     final long requestStartTime,
                     final long clientUtcOffset,
                     final boolean newPartyId,
                     final boolean firstInSession,
                     final Optional<String> location,
                     final Optional<String> referer,
                     final Optional<String> eventType,
                     final Optional<Integer> viewportPixelWidth,
                     final Optional<Integer> viewportPixelHeight,
                     final Optional<Integer> screenPixelWidth,
                     final Optional<Integer> screenPixelHeight,
                     final Optional<Integer> devicePixelRatio,
                     final Function<String, Optional<String>> eventParameterProducer) {
        this.corruptEvent           = corruptEvent;
        this.partyCookie            = Objects.requireNonNull(partyCookie);
        this.sessionCookie          = Objects.requireNonNull(sessionCookie);
        this.pageViewId             = Objects.requireNonNull(pageViewId);
        this.eventId                = Objects.requireNonNull(eventId);
        this.requestStartTime       = requestStartTime;
        this.clientUtcOffset        = clientUtcOffset;
        this.newPartyId             = newPartyId;
        this.firstInSession         = firstInSession;
        this.location               = Objects.requireNonNull(location);
        this.referer                = Objects.requireNonNull(referer);
        this.eventType              = Objects.requireNonNull(eventType);
        this.viewportPixelWidth     = Objects.requireNonNull(viewportPixelWidth);
        this.viewportPixelHeight    = Objects.requireNonNull(viewportPixelHeight);
        this.screenPixelWidth       = Objects.requireNonNull(screenPixelWidth);
        this.screenPixelHeight      = Objects.requireNonNull(screenPixelHeight);
        this.devicePixelRatio       = Objects.requireNonNull(devicePixelRatio);
        this.eventParameterProducer = Objects.requireNonNull(eventParameterProducer);
    }
}
