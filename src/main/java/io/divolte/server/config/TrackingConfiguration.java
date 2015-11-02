package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class TrackingConfiguration {
    public final String partyCookie;
    public final Duration partyTimeout;
    public final String sessionCookie;
    public final Duration sessionTimeout;
    public final Optional<String> cookieDomain;
    public final UaParserConfiguration uaParser;
    public final Optional<String> ip2geoDatabase;
    public final Optional<String> schemaFile;
    public final Optional<SchemaMappingConfiguration> schemaMapping;

    @JsonCreator
    private TrackingConfiguration(
            final String partyCookie,
            final Duration partyTimeout,
            final String sessionCookie,
            final Duration sessionTimeout,
            final Optional<String> cookieDomain,
            final UaParserConfiguration uaParser,
            final Optional<String> ip2geoDatabase,
            final Optional<String> schemaFile,
            final Optional<SchemaMappingConfiguration> schemaMapping) {
        this.partyCookie = Objects.requireNonNull(partyCookie);
        this.partyTimeout = Objects.requireNonNull(partyTimeout);
        this.sessionCookie = Objects.requireNonNull(sessionCookie);
        this.sessionTimeout = Objects.requireNonNull(sessionTimeout);
        this.cookieDomain = Objects.requireNonNull(cookieDomain);
        this.uaParser = Objects.requireNonNull(uaParser);
        this.ip2geoDatabase = Objects.requireNonNull(ip2geoDatabase);
        this.schemaFile = Objects.requireNonNull(schemaFile);
        this.schemaMapping = Objects.requireNonNull(schemaMapping);
    }

    @Override
    public String toString() {
        return "TrackingConfiguration [partyCookie=" + partyCookie + ", partyTimeout=" + partyTimeout + ", sessionCookie=" + sessionCookie + ", sessionTimeout=" + sessionTimeout + ", cookieDomain=" + cookieDomain + ", uaParser=" + uaParser + ", ip2geoDatabase=" + ip2geoDatabase + ", schemaFile=" + schemaFile + ", schemaMapping=" + schemaMapping + "]";
    }
}
