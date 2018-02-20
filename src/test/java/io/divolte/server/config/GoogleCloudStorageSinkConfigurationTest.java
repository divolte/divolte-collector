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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GoogleCloudStorageSinkConfigurationTest {

    private static double DEFAULT_DELTA = 1e-7;

    @Test
    public void testRetryConfigurationValid() {
        final ValidatedConfiguration vc = new ValidatedConfiguration(() -> ConfigFactory.parseResources(
            "gcs-sink.conf"));
        // Check that we can generate settings from our defaults.
        GoogleCloudStorageSinkConfiguration gcsConfig = (GoogleCloudStorageSinkConfiguration) vc.configuration().sinks
            .get("gcs");

        assertEquals("gs://bucket/folder", gcsConfig.bucket);

        RetryPolicy policy = gcsConfig.createRetryPolicy();

        assertNotNull(policy);

        assertEquals(1925, policy.getJitter().toMillis(), DEFAULT_DELTA);
        assertEquals(8, policy.getMaxRetries());
        assertEquals(2.2, policy.getDelayFactor(), DEFAULT_DELTA);
        assertEquals(new Duration(19, TimeUnit.SECONDS), policy.getDelay());
        assertEquals(new Duration(25, TimeUnit.SECONDS), policy.getMaxDelay());
    }
}
