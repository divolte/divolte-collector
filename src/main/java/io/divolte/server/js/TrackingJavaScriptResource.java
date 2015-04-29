/*
 * Copyright 2014 GoDataDriven B.V.
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

import io.divolte.server.ValidatedConfiguration;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

@ParametersAreNonnullByDefault
public class TrackingJavaScriptResource extends JavaScriptResource {
    private static final Logger logger = LoggerFactory.getLogger(TrackingJavaScriptResource.class);

    private static final String SCRIPT_CONSTANT_NAME = "SCRIPT_NAME";

    public TrackingJavaScriptResource(final ValidatedConfiguration vc) throws IOException {
        super("divolte.js", createScriptConstants(vc), vc.configuration().javascript.debug);
    }

    public String getScriptName() {
        return (String)getScriptConstants().get(SCRIPT_CONSTANT_NAME);
    }

    private static ImmutableMap<String, Object> createScriptConstants(final ValidatedConfiguration vc) {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("PARTY_COOKIE_NAME", vc.configuration().tracking.partyCookie);
        builder.put("PARTY_ID_TIMEOUT_SECONDS", trimLongToMaxInt(vc.configuration().tracking.partyTimeout.get(ChronoUnit.SECONDS)));
        builder.put("SESSION_COOKIE_NAME", vc.configuration().tracking.sessionCookie);
        builder.put("SESSION_ID_TIMEOUT_SECONDS", trimLongToMaxInt(vc.configuration().tracking.sessionTimeout.get(ChronoUnit.SECONDS)));
        vc.configuration().tracking.cookieDomain
                      .ifPresent((v) -> builder.put("COOKIE_DOMAIN", v));
        builder.put("LOGGING", vc.configuration().javascript.logging);
        builder.put(SCRIPT_CONSTANT_NAME, vc.configuration().javascript.name);
        builder.put("AUTO_PAGE_VIEW_EVENT", vc.configuration().javascript.autoPageViewEvent);
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
}
