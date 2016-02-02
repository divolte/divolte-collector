package io.divolte.server.config;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
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

    public static final BrowserSourceConfiguration DEFAULT_BROWSER_SOURCE_CONFIGURATION = new BrowserSourceConfiguration(
            DEFAULT_PREFIX,
            Optional.empty(),
            DEFAULT_PARTY_COOKIE,
            DurationDeserializer.parseDuration(DEFAULT_PARTY_TIMEOUT),
            DEFAULT_SESSION_COOKIE,
            DurationDeserializer.parseDuration(DEFAULT_SESSION_TIMEOUT),
            JavascriptConfiguration.DEFAULT_JAVASCRIPT_CONFIGURATION);

    public final Optional<String> cookieDomain;
    public final String partyCookie;
    public final Duration partyTimeout;
    public final String sessionCookie;
    public final Duration sessionTimeout;

    @Valid
    public final JavascriptConfiguration javascript;

    @JsonCreator
    BrowserSourceConfiguration(@JsonProperty(defaultValue=DEFAULT_PREFIX) final String prefix,
                               final Optional<String> cookieDomain,
                               @JsonProperty(defaultValue=DEFAULT_PARTY_COOKIE) final String partyCookie,
                               @JsonProperty(defaultValue=DEFAULT_PARTY_TIMEOUT) final Duration partyTimeout,
                               @JsonProperty(defaultValue=DEFAULT_SESSION_COOKIE) final String sessionCookie,
                               @JsonProperty(defaultValue=DEFAULT_SESSION_TIMEOUT) final Duration sessionTimeout,
                               final JavascriptConfiguration javascript) {
        super(prefix);
        // TODO: register a custom deserializer with Jackson that uses the defaultValue proprty from the annotation to fix this
        this.cookieDomain = cookieDomain;
        this.partyCookie = partyCookie == null ? DEFAULT_PARTY_COOKIE : partyCookie;
        this.partyTimeout = partyTimeout == null ? DurationDeserializer.parseDuration(DEFAULT_PARTY_TIMEOUT) : partyTimeout;
        this.sessionCookie = sessionCookie == null ? DEFAULT_SESSION_COOKIE : sessionCookie;
        this.sessionTimeout = sessionTimeout == null ? DurationDeserializer.parseDuration(DEFAULT_SESSION_TIMEOUT) : sessionTimeout;
        this.javascript = Optional.ofNullable(javascript).orElse(JavascriptConfiguration.DEFAULT_JAVASCRIPT_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("cookieDomain", cookieDomain)
                .add("partyCookie", partyCookie)
                .add("partyTimeout", partyTimeout)
                .add("sessionCookie", sessionCookie)
                .add("sessionTimeout", sessionTimeout)
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
