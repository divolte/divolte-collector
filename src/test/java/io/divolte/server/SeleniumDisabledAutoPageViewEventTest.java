/*
 * Copyright 2017 GoDataDriven B.V.
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

package io.divolte.server;

import static io.divolte.server.SeleniumTestBase.TestPages.*;
import static org.junit.Assert.*;

import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

import io.divolte.server.ServerTestUtils.EventPayload;

@ParametersAreNonnullByDefault
public class SeleniumDisabledAutoPageViewEventTest extends SeleniumTestBase {
    @Before
    public void setup() throws Exception {
        doSetUp("selenium-test-no-default-event-config.conf");
    }

    @Test
    public void shouldFireOnlyCustomPageViewEvent() throws InterruptedException {
        Preconditions.checkState(null != server);

        gotoPage(CUSTOM_PAGE_VIEW);

        final EventPayload payload = server.waitForEvent();

        final DivolteEvent eventData = payload.event;
        final Optional<String> eventParameters = eventData.eventParametersProducer.get().map(Object::toString);
        assertTrue(eventParameters.isPresent());
        assertEquals("{\"foo\":\"moo\",\"bar\":\"baz\"}", eventParameters.get());
        assertFalse(server.eventsRemaining());
    }
}
