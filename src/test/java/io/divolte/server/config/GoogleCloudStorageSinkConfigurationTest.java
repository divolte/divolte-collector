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

package io.divolte.server.config;

import com.typesafe.config.ConfigFactory;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.util.Duration;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class GoogleCloudStorageSinkConfigurationTest {
    private static double DEFAULT_DELTA = 1e-7;

    @Test
    public void testDefaultRetryConfigurationValid() {
        // Check that we can generate settings from our defaults.
        final RetryPolicy retryPolicy =
            GoogleCloudStorageSinkConfiguration.DEFAULT_RETRY_SETTINGS.createRetryPolicy();
        assertNotNull(retryPolicy);
    }

    @Test
    public void testSpecifyingBothJitterSettingsInvalid() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("gcs-both-jitter-invalid.conf"));

        // Verify that the configuration is invalid, and the error message is as expected.
        assertFalse(vc.isValid());
        assertEquals(1, vc.errors().size());
        assertTrue(vc.errors()
                     .get(0)
                     .startsWith("Property 'divolte.sinks[gcs].retrySettings' Retry settings may specify a jitter duration or factor, but not both.."));
    }

    @Test
    public void testJitterFactorSuppressesJitterDelayDefault() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources("gcs-jitter-factor.conf"));
        assertTrue(vc.isValid());

        final GoogleCloudStorageRetryConfiguration retrySettings = vc.configuration().getSinkConfiguration("gcs", GoogleCloudStorageSinkConfiguration.class).retrySettings;
        assertEquals(Optional.empty(), retrySettings.jitterDelay);
        assertEquals(0.1, retrySettings.jitterFactor.orElseThrow(IllegalStateException::new), DEFAULT_DELTA);
    }

    @Test
    public void testRetryConfiguration() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources(
            "gcs-sink.conf"));

        // Check that we generate the retry policy that matches the settings.
        final GoogleCloudStorageRetryConfiguration retrySettings = vc.configuration().getSinkConfiguration("gcs", GoogleCloudStorageSinkConfiguration.class).retrySettings;
        final RetryPolicy retryPolicy = retrySettings.createRetryPolicy();

        assertEquals(8, retryPolicy.getMaxRetries());
        assertEquals(new Duration(138, TimeUnit.SECONDS), retryPolicy.getMaxDuration());
        assertEquals(new Duration(19, TimeUnit.SECONDS), retryPolicy.getDelay());
        assertEquals(2.2, retryPolicy.getDelayFactor(), DEFAULT_DELTA);
        assertEquals(new Duration(25, TimeUnit.SECONDS), retryPolicy.getMaxDelay());
        assertEquals(1925, retryPolicy.getJitter().toMillis(), DEFAULT_DELTA);
    }
}
