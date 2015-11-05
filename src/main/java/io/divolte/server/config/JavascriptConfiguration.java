package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.MoreObjects;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.Optional;

@ParametersAreNonnullByDefault
public final class JavascriptConfiguration {
    private static final String DEFAULT_NAME = "divolte.js";
    private static final boolean DEFAULT_LOGGING = false;
    private static final boolean DEFAULT_DEBUG = false;
    private static final boolean DEFAULT_AUTO_PAGE_VIEW_EVENT = false;

    static final JavascriptConfiguration DEFAULT_JAVASCRIPT_CONFIGURATION =
            new JavascriptConfiguration(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    @NotNull @NotEmpty @Pattern(regexp="^[A-Za-z0-9_-]+\\.js$")
    public final String name;

    public final boolean logging;
    public final boolean debug;
    public final boolean autoPageViewEvent;

    @JsonCreator
    JavascriptConfiguration(final Optional<String> name,
                            final Optional<Boolean> logging,
                            final Optional<Boolean> debug,
                            final Optional<Boolean> autoPageViewEvent) {
        this.name = name.orElse(DEFAULT_NAME);
        this.logging = logging.orElse(DEFAULT_LOGGING);
        this.debug = debug.orElse(DEFAULT_DEBUG);
        this.autoPageViewEvent = autoPageViewEvent.orElse(DEFAULT_AUTO_PAGE_VIEW_EVENT);
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
