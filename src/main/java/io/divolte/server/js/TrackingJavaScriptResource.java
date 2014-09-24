package io.divolte.server.js;

import io.divolte.server.OptionalConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

@ParametersAreNonnullByDefault
public class TrackingJavaScriptResource extends JavaScriptResource {
    private static final Logger logger = LoggerFactory.getLogger(TrackingJavaScriptResource.class);

    public TrackingJavaScriptResource(final Config config) throws IOException {
        super("static/dvt.js", createScriptConstants(config), getJavascriptDebugMode(config));
    }

    private static ImmutableMap<String, Object> createScriptConstants(final Config config) {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("PARTY_COOKIE_NAME", config.getString("divolte.tracking.party_cookie"));
        builder.put("PARTY_ID_TIMEOUT_SECONDS", getDurationSeconds(config, "divolte.tracking.party_timeout"));
        builder.put("SESSION_COOKIE_NAME", config.getString("divolte.tracking.session_cookie"));
        builder.put("SESSION_ID_TIMEOUT_SECONDS", getDurationSeconds(config, "divolte.tracking.session_timeout"));
        OptionalConfig.of(config::getString, "divolte.tracking.cookie_domain")
                      .ifPresent((v) -> builder.put("COOKIE_DOMAIN", v));
        builder.put("LOGGING", config.getBoolean("divolte.javascript.logging"));
        return builder.build();
    }

    private static int getDurationSeconds(final Config config, final String path) {
        final long duration = config.getDuration(path, TimeUnit.SECONDS);
        final int result;
        if (duration <= Integer.MAX_VALUE) {
            result = (int)duration;
        } else {
            result = Integer.MAX_VALUE;
            logger.warn("Configured duration ({}) is too higher; capping at {}.",
                        duration, result);
        }
        return result;
    }

    private static boolean getJavascriptDebugMode(final Config config) {
        return config.getBoolean("divolte.javascript.debuggable");
    }
}
