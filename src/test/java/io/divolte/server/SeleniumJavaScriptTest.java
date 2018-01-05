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

package io.divolte.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.config.BrowserSourceConfiguration;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.divolte.server.IncomingRequestProcessor.DUPLICATE_EVENT_KEY;
import static io.divolte.server.SeleniumTestBase.TestPages.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
public class SeleniumJavaScriptTest extends SeleniumTestBase {
    private static final Logger logger = LoggerFactory.getLogger(SeleniumJavaScriptTest.class);

    @Test
    public void shouldRegenerateIDsOnExplicitNavigation() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server);

        // do a sequence of explicit navigation by setting the browser location
        // and then check that all requests generated a unique pageview ID
        final Runnable[] actions = {
                () -> gotoPage(BASIC),
                () -> gotoPage(BASIC_COPY),
                () -> gotoPage(BASIC),
                };

        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnRefresh() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> gotoPage(BASIC),
                driver.navigate()::refresh
                };
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnBackNavigation() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> gotoPage(BASIC),
                () -> gotoPage(BASIC_COPY),
                driver.navigate()::back,
        };
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnForwardNavigation() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> gotoPage(BASIC),
                () -> gotoPage(BASIC_COPY),
                driver.navigate()::back,
                driver.navigate()::forward,
        };
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldSignalWhenLateLoaded() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);

        // Go to the page but don't load Divolte immediately.
        gotoPage(PAGE_VIEW_DEFERRED_LOAD, false);
        final WebDriverWait wait = new WebDriverWait(driver, 30);
        final WebElement loadButton = wait.until(ExpectedConditions.presenceOfElementLocated(By.id("load_divolte")));

        logger.info("Triggering deferred loading of Divolte");
        loadButton.click();

        // Wait for the page-view event to arrive.
        final EventPayload payload = server.waitForEvent();
        final DivolteEvent eventData = payload.event;

        assertEquals(Optional.of("pageView"), eventData.eventType);
    }

    @Test
    public void shouldGenerateIDsOnComplexSeriesOfEvents() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> gotoPage(BASIC),
                () -> gotoPage(BASIC_COPY),
                () -> gotoPage(BASIC),
                () -> gotoPage(BASIC_COPY),
                () -> gotoPage(BASIC),
                driver.navigate()::back,
                driver.navigate()::back,
                () -> driver.findElement(By.id("custom")).click(),
                driver.navigate()::forward,
                driver.navigate()::refresh,
                driver.navigate()::back,
                () -> gotoPage(PAGE_VIEW_SUPPLIED),
                driver.navigate()::back
                };

        // We expect one duplicate PV ID, because of the custom event
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length - 1, numberOfUniquePageViewIDs);
    }

    private int uniquePageViewIdsForSeriesOfActions(final Runnable[] actions) {
        Preconditions.checkState(null != server);
        logger.info("Starting sequence of {} browser actions.", actions.length);
        return IntStream.range(0, actions.length)
               .mapToObj((index) -> {
                   final Runnable action = actions[index];
                   logger.info("Issuing browser action #{}.", index);
                   action.run();
                   logger.debug("Waiting for event from server.");
                   final EventPayload payload = unchecked(server::waitForEvent);
                   final DivolteEvent event = payload.event;
                   final Optional<String> pageViewId = event.browserEventData.map(b -> b.pageViewId);
                   logger.info("Browser action #{} yielded pageview/event: {}/{}", index, pageViewId, event.eventId);
                   return pageViewId;
               })
               .flatMap((pageViewId) -> pageViewId.map(Stream::of).orElse(null))
               .collect(Collectors.toSet()).size();
    }

    @FunctionalInterface
    private interface ExceptionSupplier<T> {
        T supply() throws Exception;
    }

    private static <T> T unchecked(final ExceptionSupplier<T> supplier) {
        try {
            return supplier.supply();
        } catch (final RuntimeException e) {
            // Pass through as-is;
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldSignalWhenOpeningPage() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server);

        final String location = gotoPage(BASIC);

        final EventPayload payload = server.waitForEvent();

        final DivolteEvent eventData = payload.event;
        final Boolean detectedDuplicate = payload.event.exchange.getAttachment(DUPLICATE_EVENT_KEY);

        assertFalse(eventData.corruptEvent);
        assertFalse(detectedDuplicate);

        assertFalse(Strings.isNullOrEmpty(eventData.partyId.value));

        assertTrue(eventData.newPartyId);

        assertFalse(Strings.isNullOrEmpty(eventData.sessionId.value));
        assertTrue(eventData.firstInSession);

        assertTrue(eventData.browserEventData.isPresent());
        final DivolteEvent.BrowserEventData browserEventData = eventData.browserEventData.get();
        assertFalse(Strings.isNullOrEmpty(browserEventData.pageViewId));
        assertFalse(Strings.isNullOrEmpty(eventData.eventId));

        assertTrue(eventData.eventType.isPresent());
        assertEquals("pageView", eventData.eventType.get());

        assertTrue(browserEventData.location.isPresent());
        assertEquals(location, browserEventData.location.get());

        /*
         * We don't really know anything about the clock on the executing browser,
         * but we'd expect it to be a reasonably accurate clock on the same planet.
         * So, if it is within +/- 12 hours of our clock, we think it's fine.
         */
        final Instant now = Instant.now();
        assertThat(eventData.clientTime,
                   allOf(greaterThan(now.minus(12, ChronoUnit.HOURS)), lessThan(now.plus(12, ChronoUnit.HOURS))));

        /*
         * Doing true assertions against the viewport and window size
         * is problematic on different devices, as the number do not
         * always make sense on SauceLabs. Also, sometimes the window
         * is partially outside of the screen view port or other strange
         * things. It gets additionally complicated on mobile devices.
         *
         * Hence, we just check whether these are integers greater than 50.
         */
        assertTrue(browserEventData.viewportPixelWidth.isPresent());
        assertThat(browserEventData.viewportPixelWidth.get(), greaterThan(50));

        assertTrue(browserEventData.viewportPixelHeight.isPresent());
        assertThat(browserEventData.viewportPixelHeight.get(), greaterThan(50));

        assertTrue(browserEventData.screenPixelWidth.isPresent());
        assertThat(browserEventData.screenPixelWidth.get(), greaterThan(50));

        assertTrue(browserEventData.screenPixelHeight.isPresent());
        assertThat(browserEventData.screenPixelHeight.get(), greaterThan(50));
    }

    @Test
    public void shouldSendCustomEvent() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);
        gotoPage(BASIC);
        server.waitForEvent();

        driver.findElement(By.id("custom")).click();
        final EventPayload payload = server.waitForEvent();
        final DivolteEvent eventData = payload.event;

        assertTrue(eventData.eventType.isPresent());
        assertEquals("custom", eventData.eventType.get());

        final Optional<String> customEventParameters =
                eventData.eventParametersProducer.get().map(Object::toString);
        assertTrue(customEventParameters.isPresent());
        assertEquals("{\"a\":{}," +
                     "\"b\":\"c\"," +
                     "\"d\":{\"a\":[],\"b\":\"g\"}," +
                     "\"e\":[\"1\",\"2\"]," +
                     "\"f\":42," +
                     "\"g\":53.2," +
                     "\"h\":-37," +
                     "\"i\":-7.83E-9," +
                     "\"j\":true," +
                     "\"k\":false," +
                     "\"l\":null," +
                     "\"m\":\"2015-06-13T15:49:33.002Z\"," +
                     "\"n\":{}," +
                     "\"o\":[{},{\"a\":\"b\"},{\"c\":\"d\"}]," +
                     "\"p\":[null,null,{\"a\":\"b\"},\"custom\",null,{}]," +
                     "\"q\":{}}",
                     customEventParameters.get());
    }

    @Test
    public void shouldSetAppropriateCookies() throws Exception {
        doSetUp();
        Preconditions.checkState(null != driver && null != server);
        gotoPage(BASIC);
        server.waitForEvent();

        final Optional<DivolteIdentifier> parsedPartyCookieOption = DivolteIdentifier.tryParse(driver.manage().getCookieNamed(BrowserSourceConfiguration.DEFAULT_BROWSER_SOURCE_CONFIGURATION.partyCookie).getValue());
        assertTrue(parsedPartyCookieOption.isPresent());
        assertThat(
                parsedPartyCookieOption.get(),
                isA(DivolteIdentifier.class));

        final Optional<DivolteIdentifier> parsedSessionCookieOption = DivolteIdentifier.tryParse(driver.manage().getCookieNamed(BrowserSourceConfiguration.DEFAULT_BROWSER_SOURCE_CONFIGURATION.sessionCookie).getValue());
        assertTrue(parsedSessionCookieOption.isPresent());
        assertThat(
                parsedSessionCookieOption.get(),
                isA(DivolteIdentifier.class));
    }

    @Test
    public void shouldPickupProvidedPageViewIdFromHash() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server);
        gotoPage(PAGE_VIEW_SUPPLIED);
        final EventPayload payload = server.waitForEvent();
        final DivolteEvent eventData = payload.event;

        assertEquals(Optional.of("supercalifragilisticexpialidocious"), eventData.browserEventData.map(bed -> bed.pageViewId));
        assertEquals("supercalifragilisticexpialidocious0", eventData.eventId);
    }

    @Test
    public void shouldSupportCustomJavascriptName() throws Exception {
        doSetUp("selenium-test-custom-javascript-name.conf");
        Preconditions.checkState(null != server);

        gotoPage(CUSTOM_JAVASCRIPT_NAME);
        final EventPayload payload = server.waitForEvent();
        final DivolteEvent eventData = payload.event;

        assertEquals(Optional.of("pageView"), eventData.eventType);
    }

    @Test
    public void shouldUseConfiguredEventSuffix() throws Exception {
        doSetUp("selenium-test-custom-event-suffix.conf");
        Preconditions.checkState(null != server);
        gotoPage(BASIC);
        final EventPayload payload = server.waitForEvent();
        final DivolteEvent eventData = payload.event;

        assertEquals(Optional.of("pageView"), eventData.eventType);
    }

    @Test
    public void shouldAdvanceToNextEventOnTimeout() throws Exception {
        doSetUp("selenium-test-slow-server.conf");
        Preconditions.checkState(null != server && null != driver);
        gotoPage(BASIC);
        // No page-view events.

        // Emit two events.
        logger.info("Clicking link that will trigger 2 slow events");
        driver.findElement(By.id("deliver2events")).click();
        /*
         * The server has been configured to delay responses by 15 seconds.
         * The JavaScript should time them out quickly.
         * This means both events should arrive within the time here,
         * instead of waiting for the delayed responses.
         */
        logger.info("Waiting for first event");
        server.waitForEvent();
        logger.info("First event received; waiting for second event.");
        server.waitForEvent();
        logger.info("Second event received.");
    }

    @Test
    public void shouldFlushEventsBeforeClickOut() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server && null != driver);
        gotoPage(EVENT_COMMIT);
        assertEquals(Optional.of("pageView"), server.waitForEvent().event.eventType);

        // Record the current page URL, so that later we can check we navigated away.
        final String initialPageUrl = driver.getCurrentUrl();

        // Locate the link to click on, and click it.
        logger.info("Clicking link that will trigger events");
        driver.findElement(By.id("clickout")).click();

        // At this point 10 events should arrive, after which the browser navigates
        // to the basic page.
        for (int i = 0; i < 10; ++i) {
            final DivolteEvent eventData = server.waitForEvent().event;
            assertEquals("Unexpected event type for event #" + i,
                         Optional.of("clickOutEvent"),
                         eventData.eventType);
            assertEquals("Event index parameter incorrect for event #" + i,
                         Optional.of(i),
                         eventData.eventParametersProducer.get().map(jsonNode -> jsonNode.get("i").asInt()));
        }
        logger.info("Triggered events have all arrived.");

        // Check that the browser did navigate to a new page. (It will also generate a page-view.)
        final WebDriverWait wait = new WebDriverWait(driver, 30);
        wait.until(driver -> !driver.getCurrentUrl().equals(initialPageUrl));
        assertEquals(Optional.of("pageView"), server.waitForEvent().event.eventType);
    }

    @Test
    public void shouldInvokeFlushCallbackImmediatelyIfNoEventsPending() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server && null != driver);
        gotoPage(EVENT_COMMIT);
        assertEquals(Optional.of("pageView"), server.waitForEvent().event.eventType);

        // Hit the 'flush' link, and wait for the event.
        driver.findElement(By.id("flushdirect")).click();

        // The event contains information about whether the callback was invoked immediately or not.
        final DivolteEvent event = server.waitForEvent().event;
        assertEquals(Optional.of("firstFlush"), event.eventType);
        assertEquals(Optional.of(true),
                     event.eventParametersProducer.get().map(jsonNode -> jsonNode.get("callbackWasImmediate").asBoolean()));
    }

    @Test
    public void shouldTimeoutIfRequiredOnFlushBeforeClickOut() throws Exception {
        doSetUp();
        Preconditions.checkState(null != server && null != driver);
        gotoPage(EVENT_COMMIT);
        assertEquals(Optional.of("pageView"), server.waitForEvent().event.eventType);

        // Record the current page URL, so that later we can check we navigated away.
        final String initialPageUrl = driver.getCurrentUrl();

        // Locate the link to click on, and click it.
        logger.info("Clicking link that will trigger events");
        driver.findElement(By.id("quickout")).click();

        // At this point lots of events should arrive, but before we reach 100000 the
        // flush should have timed-out and navigated to the new page.
        int count = 0;
        DivolteEvent lastEventData;
        do {
            lastEventData = server.waitForEvent().event;
            if (!Optional.of("clickOutEvent").equals(lastEventData.eventType)) {
                break;
            }
            ++count;
        } while (count < 1000);
        assertThat(count, is(both(greaterThan(0)).and(lessThan(1000))));
        logger.info("Triggered events have all arrived: {}", count);

        // Check that the browser did navigate to a new page. It will have generated a new page-view.
        final WebDriverWait wait = new WebDriverWait(driver, 10);
        wait.until(driver -> !driver.getCurrentUrl().equals(initialPageUrl));
        assertEquals(Optional.of("pageView"), lastEventData.eventType);
    }
}
