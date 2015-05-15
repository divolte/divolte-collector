package io.divolte.server;

import com.google.common.base.Preconditions;
import io.divolte.server.ServerTestUtils.EventPayload;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

import static io.divolte.server.IncomingRequestProcessor.DIVOLTE_EVENT_KEY;
import static io.divolte.server.SeleniumTestBase.TEST_PAGES.CUSTOM_PAGE_VIEW;
import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
public class SeleniumDisabledAutoPageViewEventTest extends SeleniumTestBase {
    @Before
    public void setup() throws Exception {
        doSetUp("selenium-test-no-default-event-config.conf");
    }

    @Test
    public void shouldFireOnlyCustomPageViewEvent() throws InterruptedException {
        Preconditions.checkState(null != driver && null != server);

        final String location = urlOf(CUSTOM_PAGE_VIEW);
        driver.get(location);

        EventPayload viewEvent = server.waitForEvent();

        final DivolteEvent eventData = viewEvent.exchange.getAttachment(DIVOLTE_EVENT_KEY);
        final Optional<String> eventParameters = eventData.eventParametersProducer.get().map(Object::toString);
        assertTrue(eventParameters.isPresent());
        assertEquals("{\"foo\":\"moo\",\"bar\":\"baz\"}", eventParameters.get());
        assertFalse(server.eventsRemaining());
    }
}
