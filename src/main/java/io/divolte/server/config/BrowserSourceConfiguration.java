package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class BrowserSourceConfiguration extends SourceConfiguration {
    private static final String DEFAULT_PREFIX = "/";
    private static final String DEFAULT_PARTY_COOKIE = "_dvp";
    private static final Duration DEFAULT_PARTY_TIMEOUT = Duration.ofDays(730);
    private static final String DEFAULT_SESSION_COOKIE = "_dvs";
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofMinutes(30);

    public final String prefix;

    public final Optional<String> cookieDomain;
    public final String partyCookie;
    public final Duration partyTimeout;
    public final String sessionCookie;
    public final Duration sessionTimeout;

    @Valid
    public final JavascriptConfiguration javascript;

    @JsonCreator
    BrowserSourceConfiguration(final Optional<String> prefix,
                               final Optional<String> cookieDomain,
                               final Optional<String> partyCookie,
                               final Optional<Duration> partyTimeout,
                               final Optional<String> sessionCookie,
                               final Optional<Duration> sessionTimeout,
                               final Optional<JavascriptConfiguration> javascript) {
        this.prefix = prefix.orElse(DEFAULT_PREFIX);
        this.cookieDomain = Objects.requireNonNull(cookieDomain);
        this.partyCookie = partyCookie.orElse(DEFAULT_PARTY_COOKIE);
        this.partyTimeout = partyTimeout.orElse(DEFAULT_PARTY_TIMEOUT);
        this.sessionCookie = sessionCookie.orElse(DEFAULT_SESSION_COOKIE);
        this.sessionTimeout = sessionTimeout.orElse(DEFAULT_SESSION_TIMEOUT);
        this.javascript = javascript.orElse(JavascriptConfiguration.DEFAULT_JAVASCRIPT_CONFIGURATION);
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("prefix", prefix)
                .add("cookieDomain", cookieDomain)
                .add("partyCookie", partyCookie)
                .add("partyTimeout", partyTimeout)
                .add("sessionCookie", sessionCookie)
                .add("sessionTimeout", sessionTimeout)
                .add("javascript", javascript);
    }
}
