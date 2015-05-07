package io.divolte.server;

import static io.divolte.server.IncomingRequestProcessor.*;
import static io.divolte.server.SeleniumTestBase.TEST_PAGES.*;
import static org.junit.Assert.*;
import io.divolte.server.ServerTestUtils.EventPayload;

import javax.annotation.ParametersAreNonnullByDefault;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;

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
        assertEquals("moo", eventData.eventParameterProducer.apply("foo").get());
        assertEquals("baz", eventData.eventParameterProducer.apply("bar").get());
        assertFalse(server.eventsRemaining());
    }
}
