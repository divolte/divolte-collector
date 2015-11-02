package io.divolte.server.config;

import javax.annotation.ParametersAreNullableByDefault;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;

@ParametersAreNullableByDefault
public final class JavascriptConfiguration {
    @NotNull @NotEmpty @Pattern(regexp="^[A-Za-z0-9_-]+\\.js$")
    public final String name;

    public final boolean logging;
    public final boolean debug;
    public final boolean autoPageViewEvent;

    @JsonCreator
    private JavascriptConfiguration(
            final String name,
            final boolean logging,
            final boolean debug,
            final boolean autoPageViewEvent) {
        this.name = name;
        this.logging = logging;
        this.debug = debug;
        this.autoPageViewEvent = autoPageViewEvent;
    }

    @Override
    public String toString() {
        return "JavascriptConfiguration [name=" + name + ", logging=" + logging + ", debug=" + debug + ", autoPageViewEvent=" + autoPageViewEvent + "]";
    }
}
