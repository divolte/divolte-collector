/*
 * Copyright 2017 GoDataDriven B.V.
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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.divolte.server.BrowserSource;
import io.divolte.server.HttpSource;
import io.divolte.server.IncomingRequestProcessingPool;

@ParametersAreNonnullByDefault
public class BrowserSourceConfiguration extends SourceConfiguration {
    private static final String DEFAULT_PARTY_COOKIE = "_dvp";
    private static final String DEFAULT_PARTY_TIMEOUT = "730 days";
    private static final String DEFAULT_SESSION_COOKIE = "_dvs";
    private static final String DEFAULT_SESSION_TIMEOUT = "30 minutes";
    private static final String DEFAULT_PREFIX = "/";
    private static final String DEFAULT_EVENT_SUFFIX = "csc-event";
    private static final String DEFAULT_HTTP_RESPONSE_DELAY = "0 seconds";

    public static final BrowserSourceConfiguration DEFAULT_BROWSER_SOURCE_CONFIGURATION = new BrowserSourceConfiguration(
            DEFAULT_PREFIX,
            DEFAULT_EVENT_SUFFIX,
            Optional.empty(),
            DEFAULT_PARTY_COOKIE,
            DurationDeserializer.parseDuration(DEFAULT_PARTY_TIMEOUT),
            DEFAULT_SESSION_COOKIE,
            DurationDeserializer.parseDuration(DEFAULT_SESSION_TIMEOUT),
            DurationDeserializer.parseDuration(DEFAULT_HTTP_RESPONSE_DELAY),
            JavascriptConfiguration.DEFAULT_JAVASCRIPT_CONFIGURATION);

    public final String prefix;
    public final String eventSuffix;
    public final Optional<String> cookieDomain;
    public final String partyCookie;
    public final Duration partyTimeout;
    public final String sessionCookie;
    public final Duration sessionTimeout;
    public final Duration httpResponseDelay;

    @Valid
    public final JavascriptConfiguration javascript;

    @JsonCreator
    @ParametersAreNullableByDefault
    BrowserSourceConfiguration(@JsonProperty(defaultValue=DEFAULT_PREFIX) final String prefix,
                               @JsonProperty(defaultValue=DEFAULT_EVENT_SUFFIX) final String eventSuffix,
                               @Nonnull final Optional<String> cookieDomain,
                               @JsonProperty(defaultValue=DEFAULT_PARTY_COOKIE) final String partyCookie,
                               @JsonProperty(defaultValue=DEFAULT_PARTY_TIMEOUT) final Duration partyTimeout,
                               @JsonProperty(defaultValue=DEFAULT_SESSION_COOKIE) final String sessionCookie,
                               @JsonProperty(defaultValue=DEFAULT_SESSION_TIMEOUT) final Duration sessionTimeout,
                               @JsonProperty(defaultValue=DEFAULT_HTTP_RESPONSE_DELAY) final Duration httpResponseDelay,
                               final JavascriptConfiguration javascript) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.prefix = Optional.ofNullable(prefix).map(BrowserSourceConfiguration::ensureTrailingSlash).orElse(DEFAULT_PREFIX);
        this.eventSuffix = Optional.ofNullable(eventSuffix).orElse(DEFAULT_EVENT_SUFFIX);
        this.cookieDomain = Objects.requireNonNull(cookieDomain);
        this.partyCookie = Optional.ofNullable(partyCookie).orElse(DEFAULT_PARTY_COOKIE);
        this.partyTimeout = Optional.ofNullable(partyTimeout).orElseGet(() -> DurationDeserializer.parseDuration(DEFAULT_PARTY_TIMEOUT));
        this.sessionCookie = Optional.ofNullable(sessionCookie).orElse(DEFAULT_SESSION_COOKIE);
        this.sessionTimeout = Optional.ofNullable(sessionTimeout).orElseGet(() -> DurationDeserializer.parseDuration(DEFAULT_SESSION_TIMEOUT));
        this.httpResponseDelay = Optional.ofNullable(httpResponseDelay).orElseGet(() -> DurationDeserializer.parseDuration(DEFAULT_HTTP_RESPONSE_DELAY));
        this.javascript = Optional.ofNullable(javascript).orElse(JavascriptConfiguration.DEFAULT_JAVASCRIPT_CONFIGURATION);
    }

    private static String ensureTrailingSlash(final String s) {
        return s.endsWith("/") ? s : s + '/';
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("prefix", prefix)
                .add("eventSuffix", eventSuffix)
                .add("cookieDomain", cookieDomain)
                .add("partyCookie", partyCookie)
                .add("partyTimeout", partyTimeout)
                .add("sessionCookie", sessionCookie)
                .add("sessionTimeout", sessionTimeout)
                .add("httpResponseDelay", httpResponseDelay)
                .add("javascript", javascript);
    }

    @Override
    public HttpSource createSource(
            final ValidatedConfiguration vc,
            final String sourceName,
            final IncomingRequestProcessingPool processingPool) {
        return new BrowserSource(vc, sourceName, processingPool);
    }
}
