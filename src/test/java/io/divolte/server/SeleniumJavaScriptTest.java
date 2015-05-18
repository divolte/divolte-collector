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

package io.divolte.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.divolte.server.ServerTestUtils.EventPayload;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.divolte.server.IncomingRequestProcessor.*;
import static io.divolte.server.SeleniumTestBase.TEST_PAGES.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@ParametersAreNonnullByDefault
public class SeleniumJavaScriptTest extends SeleniumTestBase {
    private static final long HALF_DAY_MS = TimeUnit.HOURS.toMillis(12);

    @Test
    public void shouldRegenerateIDsOnExplicitNavigation() {
        Preconditions.checkState(null != driver && null != server);

        // do a sequence of explicit navigation by setting the browser location
        // and then check that all requests generated a unique pageview ID
        final Runnable[] actions = {
                () -> driver.navigate().to(urlOf(BASIC)),
                () -> driver.navigate().to(urlOf(BASIC_COPY)),
                () -> driver.navigate().to(urlOf(BASIC))
                };

        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnRefresh() {
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> driver.get(urlOf(BASIC)),
                driver.navigate()::refresh
                };
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnForwardBackNavigation() {
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> driver.get(urlOf(BASIC)),
                () -> driver.get(urlOf(BASIC_COPY)),
                () -> driver.get(urlOf(BASIC)),
                driver.navigate()::back,
                driver.navigate()::back,
                driver.navigate()::forward,
                driver.navigate()::back,
                driver.navigate()::forward,
                driver.navigate()::forward
                };
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldGenerateIDsOnComplexSeriesOfEvents() {
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> driver.get(urlOf(BASIC)),
                () -> driver.get(urlOf(BASIC_COPY)),
                () -> driver.get(urlOf(BASIC)),
                () -> driver.get(urlOf(BASIC_COPY)),
                () -> driver.get(urlOf(BASIC)),
                driver.navigate()::back,
                driver.navigate()::back,
                () -> driver.findElement(By.id("custom")).click(),
                driver.navigate()::forward,
                driver.navigate()::refresh,
                driver.navigate()::back,
                () -> driver.get(urlOf(PAGE_VIEW_SUPPLIED)),
                driver.navigate()::back
                };

        // we expect on duplicate PV ID, because of the custom event
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length - 1, numberOfUniquePageViewIDs);
    }

    private int uniquePageViewIdsForSeriesOfActions(final Runnable[] actions) {
        return Stream.of(actions)
                .flatMap((action) -> {
                    action.run();
                    final EventPayload payload = unchecked(server::waitForEvent);
                    final DivolteEvent event = payload.exchange.getAttachment(DIVOLTE_EVENT_KEY);
                    return event.browserEventData.map(b -> b.pageViewId).map(Stream::of).orElse(null);
                })
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
    public void shouldSignalWhenOpeningPage() throws InterruptedException {
        Preconditions.checkState(null != driver && null != server);

        final String location = urlOf(BASIC);
        driver.get(location);

        EventPayload viewEvent = server.waitForEvent();

        final DivolteEvent eventData = viewEvent.exchange.getAttachment(DIVOLTE_EVENT_KEY);
        final Boolean detectedDuplicate = viewEvent.exchange.getAttachment(DUPLICATE_EVENT_KEY);

        assertFalse(eventData.corruptEvent);
        assertFalse(detectedDuplicate);

        assertFalse(Strings.isNullOrEmpty(eventData.partyCookie.value));

        assertTrue(eventData.newPartyId);

        assertFalse(Strings.isNullOrEmpty(eventData.sessionCookie.value));
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
        assertThat(eventData.clientUtcOffset,
                allOf(greaterThan(-HALF_DAY_MS), lessThan(HALF_DAY_MS)));

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
    public void shouldSendCustomEvent() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        driver.get(urlOf(BASIC));
        server.waitForEvent();

        driver.findElement(By.id("custom")).click();
        final EventPayload customEvent = server.waitForEvent();
        final DivolteEvent eventData = customEvent.exchange.getAttachment(DIVOLTE_EVENT_KEY);

        assertTrue(eventData.eventType.isPresent());
        assertEquals("custom", eventData.eventType.get());

        final Optional<String> customEventParameters =
                eventData.eventParametersProducer.get().map(Object::toString);
        assertTrue(customEventParameters.isPresent());
        assertEquals("{\"a\":{},\"b\":\"c\",\"d\":{\"a\":[],\"b\":\"g\"},\"e\":[\"1\",\"2\"],\"f\":42,\"g\":53.2,\"h\":-37,\"i\":-7.83E-9,\"j\":true,\"k\":false,\"l\":null,\"m\":\"2015-06-13T15:49:33.002Z\",\"n\":{},\"o\":[{},{\"a\":\"b\"},{\"c\":\"d\"}],\"p\":{}}",
                     customEventParameters.get());
    }

    @Test
    public void shouldSetAppropriateCookies() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        driver.get(urlOf(BASIC));
        server.waitForEvent();

        Optional<DivolteIdentifier> parsedPartyCookieOption = DivolteIdentifier.tryParse(driver.manage().getCookieNamed(server.config.getString("divolte.tracking.party_cookie")).getValue());
        assertTrue(parsedPartyCookieOption.isPresent());
        assertThat(
                parsedPartyCookieOption.get(),
                isA(DivolteIdentifier.class));

        Optional<DivolteIdentifier> parsedSessionCookieOption = DivolteIdentifier.tryParse(driver.manage().getCookieNamed(server.config.getString("divolte.tracking.session_cookie")).getValue());
        assertTrue(parsedSessionCookieOption.isPresent());
        assertThat(
                parsedSessionCookieOption.get(),
                isA(DivolteIdentifier.class));
    }

    @Test
    public void shouldPickupProvidedPageViewIdFromHash() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        driver.get(urlOf(PAGE_VIEW_SUPPLIED));
        EventPayload event = server.waitForEvent();
        DivolteEvent eventData = event.exchange.getAttachment(DIVOLTE_EVENT_KEY);

        assertEquals("supercalifragilisticexpialidocious", eventData.browserEventData.get().pageViewId);
        assertEquals("supercalifragilisticexpialidocious0", eventData.eventId);
    }

    @Before
    public void setup() throws Exception {
        doSetUp("selenium-test-config.conf");
    }
}
