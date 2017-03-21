package io.divolte.server;

import static io.divolte.server.SeleniumTestBase.TEST_PAGES.*;
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
