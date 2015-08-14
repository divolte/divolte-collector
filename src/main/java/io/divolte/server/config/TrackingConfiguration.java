package io.divolte.server.config;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.ParametersAreNullableByDefault;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
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
        this.partyCookie = partyCookie;
        this.partyTimeout = partyTimeout;
        this.sessionCookie = sessionCookie;
        this.sessionTimeout = sessionTimeout;
        this.cookieDomain = cookieDomain;
        this.uaParser = uaParser;
        this.ip2geoDatabase = ip2geoDatabase;
        this.schemaFile = schemaFile;
        this.schemaMapping = schemaMapping;
    }

    @Override
    public String toString() {
        return "TrackingConfiguration [partyCookie=" + partyCookie + ", partyTimeout=" + partyTimeout + ", sessionCookie=" + sessionCookie + ", sessionTimeout=" + sessionTimeout + ", cookieDomain=" + cookieDomain + ", uaParser=" + uaParser + ", ip2geoDatabase=" + ip2geoDatabase + ", schemaFile=" + schemaFile + ", schemaMapping=" + schemaMapping + "]";
    }
}