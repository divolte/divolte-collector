package io.divolte.server.config;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.ParametersAreNullableByDefault;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.Optional;

@ParametersAreNonnullByDefault
public final class JavascriptConfiguration {
    private static final String DEFAULT_NAME = "divolte.js";
    private static final String DEFAULT_LOGGING = "false";
    private static final String DEFAULT_DEBUG = "false";
    private static final String DEFAULT_AUTO_PAGE_VIEW_EVENT = "true";

    static final JavascriptConfiguration DEFAULT_JAVASCRIPT_CONFIGURATION =
            new JavascriptConfiguration(DEFAULT_NAME,
                                        Boolean.parseBoolean(DEFAULT_LOGGING),
                                        Boolean.parseBoolean(DEFAULT_DEBUG),
                                        Boolean.parseBoolean(DEFAULT_AUTO_PAGE_VIEW_EVENT));

    @NotNull @NotEmpty @Pattern(regexp="^[A-Za-z0-9_-]+\\.js$")
    public final String name;

    public final boolean logging;
    public final boolean debug;
    public final boolean autoPageViewEvent;

    @JsonCreator
    @ParametersAreNullableByDefault
    JavascriptConfiguration(@JsonProperty(defaultValue=DEFAULT_NAME) final String name,
                            @JsonProperty(defaultValue=DEFAULT_LOGGING) final Boolean logging,
                            @JsonProperty(defaultValue=DEFAULT_DEBUG) final Boolean debug,
                            @JsonProperty(defaultValue=DEFAULT_AUTO_PAGE_VIEW_EVENT) final Boolean autoPageViewEvent) {
        // TODO: register a custom deserializer with Jackson that uses the defaultValue property from the annotation to fix this
        this.name = Optional.ofNullable(name).orElse(DEFAULT_NAME);
        this.logging = Optional.ofNullable(logging).orElseGet(() -> Boolean.valueOf(DEFAULT_LOGGING));
        this.debug = Optional.ofNullable(debug).orElseGet(() -> Boolean.valueOf(DEFAULT_DEBUG));
        this.autoPageViewEvent = Optional.ofNullable(autoPageViewEvent).orElseGet(() -> Boolean.valueOf(DEFAULT_AUTO_PAGE_VIEW_EVENT));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("logging", logging)
                .add("debug", debug)
                .add("autoPageViewEvent", autoPageViewEvent)
                .toString();
    }
}
