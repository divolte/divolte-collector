/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.js;

import com.google.common.collect.ImmutableMap;
import io.divolte.server.config.BrowserSourceConfiguration;
import io.divolte.server.config.ValidatedConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

@ParametersAreNonnullByDefault
public class TrackingJavaScriptResource extends JavaScriptResource {
    private static final Logger logger = LoggerFactory.getLogger(TrackingJavaScriptResource.class);

    private static final int NANOS_PER_SECOND = (int) TimeUnit.SECONDS.toNanos(1);

    private static final String SCRIPT_CONSTANT_NAME = "SCRIPT_NAME";

    public TrackingJavaScriptResource(final String resourceName,
                                      final ImmutableMap<String, Object> scriptConstants,
                                      final boolean debugMode) throws IOException {
        super(resourceName, scriptConstants, debugMode);
    }

    public String getScriptName() {
        return (String)getScriptConstants().get(SCRIPT_CONSTANT_NAME);
    }

    private static ImmutableMap<String, Object> createScriptConstants(final BrowserSourceConfiguration browserSourceConfiguration) {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("PARTY_COOKIE_NAME", browserSourceConfiguration.partyCookie);
        builder.put("PARTY_ID_TIMEOUT_SECONDS", trimLongToMaxInt(browserSourceConfiguration.partyTimeout.get(ChronoUnit.SECONDS)));
        builder.put("SESSION_COOKIE_NAME", browserSourceConfiguration.sessionCookie);
        builder.put("SESSION_ID_TIMEOUT_SECONDS", trimLongToMaxInt(browserSourceConfiguration.sessionTimeout.get(ChronoUnit.SECONDS)));
        browserSourceConfiguration.cookieDomain.ifPresent(v -> builder.put("COOKIE_DOMAIN", v));
        browserSourceConfiguration.cookieSameSite.ifPresent(v -> builder.put("COOKIE_SAME_SITE", v));
        builder.put("LOGGING", browserSourceConfiguration.javascript.logging);
        builder.put(SCRIPT_CONSTANT_NAME, browserSourceConfiguration.javascript.name);
        builder.put("EVENT_SUFFIX", browserSourceConfiguration.eventSuffix);
        builder.put("AUTO_PAGE_VIEW_EVENT", browserSourceConfiguration.javascript.autoPageViewEvent);
        builder.put("EVENT_TIMEOUT_SECONDS", browserSourceConfiguration.javascript.eventTimeout.getSeconds() +
            browserSourceConfiguration.javascript.eventTimeout.getNano() / (double) NANOS_PER_SECOND);
        return builder.build();
    }

    private static int trimLongToMaxInt(long duration) {
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

    public static TrackingJavaScriptResource create(final ValidatedConfiguration vc,
                                                    final String sourceName) throws IOException {
        final BrowserSourceConfiguration browserSourceConfiguration =
            vc.configuration().getSourceConfiguration(sourceName, BrowserSourceConfiguration.class);
        return new TrackingJavaScriptResource("divolte.js",
            createScriptConstants(browserSourceConfiguration),
            browserSourceConfiguration.javascript.debug);
    }
}
