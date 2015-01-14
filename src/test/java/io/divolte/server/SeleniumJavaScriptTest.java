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

import static io.divolte.server.BrowserLists.*;
import static io.divolte.server.ClientSideCookieEventHandler.*;
import static io.divolte.server.IncomingRequestProcessor.EVENT_DATA_KEY;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.google.common.base.Strings;
import io.divolte.server.CookieValues.CookieValue;
import io.divolte.server.ServerTestUtils.EventPayload;
import io.divolte.server.ServerTestUtils.TestServer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

@RunWith(Parameterized.class)
@ParametersAreNonnullByDefault
public class SeleniumJavaScriptTest {
    public final static String DRIVER_ENV_VAR = "SELENIUM_DRIVER";
    public final static String PHANTOMJS_DRIVER = "phantomjs";
    public final static String CHROME_DRIVER = "chrome";
    public final static String SAUCE_DRIVER = "sauce";
    public final static String BS_DRIVER = "browserstack";


    public static final String SAUCE_USER_NAME_ENV_VAR = "SAUCE_USER_NAME";
    public static final String SAUCE_API_KEY_ENV_VAR = "SAUCE_API_KEY";
    public static final String SAUCE_HOST_ENV_VAR = "SAUCE_HOST";
    public static final String SAUCE_PORT_ENV_VAR = "SAUCE_PORT";

    public static final String BS_USER_NAME_ENV_VAR = "BS_USER_NAME";
    public static final String BS_API_KEY_ENV_VAR = "BS_API_KEY";

    public final static String CHROME_DRIVER_LOCATION_ENV_VAR = "CHROME_DRIVER";

    private static final long HALF_DAY_MS = TimeUnit.DAYS.toMillis(12);

    public final static DesiredCapabilities LOCAL_RUN_CAPABILITIES;
    static {
        LOCAL_RUN_CAPABILITIES = new DesiredCapabilities();
        LOCAL_RUN_CAPABILITIES.setBrowserName("Local Selenium instructed browser");
    }

    @Nullable
    private WebDriver driver;
    @Nullable
    private TestServer server;

    @Parameter(0)
    public Supplier<DesiredCapabilities> capabilities;

    @Parameter(1)
    public String capabilityDescription;

    @Parameters(name = "Selenium JS test: {1}")
    public static Iterable<Object[]> sauceLabBrowsersToTest() {
        if (!System.getenv().containsKey(DRIVER_ENV_VAR)) {
            return Collections.emptyList();
        } else if (SAUCE_DRIVER.equals(System.getenv().get(DRIVER_ENV_VAR))) {
            System.out.println("Selenium test running on SauceLabs with these browsers:\n" + browserNameList(SAUCE_BROWSER_LIST));
            return SAUCE_BROWSER_LIST;
        } else if (BS_DRIVER.equals(System.getenv().get(DRIVER_ENV_VAR))) {
            System.out.println("Selenium test running on BrowserStack with these browsers:\n" + browserNameList(BS_BROWSER_LIST));
            return BS_BROWSER_LIST;
        } else {
            // Parameters are not used for non-sauce tests
            return ImmutableList.of(new Object[] { (Supplier<DesiredCapabilities>) () -> LOCAL_RUN_CAPABILITIES, "Local JS test run" });
        }
    }

    @Test
    public void shouldRegenerateIDsOnExplicitNavigation() {
        Preconditions.checkState(null != driver && null != server);

        // do a sequence of explicit navigation by setting the browser location
        // and then check that all requests generated a unique pageview ID
        final Runnable[] actions = {
                () -> driver.navigate().to(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
                () -> driver.navigate().to(String.format("http://127.0.0.1:%d/test-basic-page-copy.html", server.port)),
                () -> driver.navigate().to(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port))
                };

        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length, numberOfUniquePageViewIDs);
    }

    @Test
    public void shouldRegenerateIDsOnRefresh() {
        Preconditions.checkState(null != driver && null != server);

        // Navigate to the same page twice
        final Runnable[] actions = {
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
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
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page-copy.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
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
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page-copy.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page-copy.html", server.port)),
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page.html", server.port)),
                driver.navigate()::back,
                driver.navigate()::back,
                () -> driver.findElement(By.id("custom")).click(),
                driver.navigate()::forward,
                driver.navigate()::refresh,
                driver.navigate()::back,
                () -> driver.get(String.format("http://127.0.0.1:%d/test-basic-page-provided-pv-id.html", server.port)),
                driver.navigate()::back
                };

        // we expect on duplicate PV ID, because of the custom event
        final int numberOfUniquePageViewIDs = uniquePageViewIdsForSeriesOfActions(actions);
        assertEquals(actions.length - 1, numberOfUniquePageViewIDs);
    }

    private int uniquePageViewIdsForSeriesOfActions(final Runnable[] actions) {
        return Stream.of(actions)
              .map((action) -> {
                  action.run();
                  final EventPayload event = unchecked(server::waitForEvent);
                  return event.exchange.getAttachment(EVENT_DATA_KEY).pageViewId;
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

        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", server.port);
        driver.get(location);

        EventPayload viewEvent = server.waitForEvent();

        final BrowserEventData eventData = viewEvent.exchange.getAttachment(EVENT_DATA_KEY);

        assertFalse(Strings.isNullOrEmpty(eventData.partyCookie.value));

        assertTrue(eventData.newPartyId);

        assertFalse(Strings.isNullOrEmpty(eventData.sessionCookie.value));
        assertTrue(eventData.firstInSession);

        assertFalse(Strings.isNullOrEmpty(eventData.pageViewId));
        assertFalse(Strings.isNullOrEmpty(eventData.eventId));

        assertTrue(eventData.eventType.isPresent());
        assertEquals("pageView", eventData.eventType.get());

        assertTrue(eventData.location.isPresent());
        assertEquals(location, eventData.location.get());

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
        assertTrue(eventData.viewportPixelWidth.isPresent());
        assertThat(eventData.viewportPixelWidth.get(), greaterThan(50));

        assertTrue(eventData.viewportPixelHeight.isPresent());
        assertThat(eventData.viewportPixelHeight.get(), greaterThan(50));

        assertTrue(eventData.screenPixelWidth.isPresent());
        assertThat(eventData.screenPixelWidth.get(), greaterThan(50));

        assertTrue(eventData.screenPixelHeight.isPresent());
        assertThat(eventData.screenPixelHeight.get(), greaterThan(50));
    }

    @Test
    public void shouldSendCustomEvent() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", server.port);
        driver.get(location);
        server.waitForEvent();

        driver.findElement(By.id("custom")).click();
        final EventPayload customEvent = server.waitForEvent();
        final BrowserEventData eventData = customEvent.exchange.getAttachment(EVENT_DATA_KEY);

        assertTrue(eventData.eventType.isPresent());
        assertEquals("custom", eventData.eventType.get());
        final Optional<String> customEventParameter = eventData.eventParameterProducer.apply("key");
        assertTrue(customEventParameter.isPresent());
        assertEquals("value", customEventParameter.get());
    }

    @Test
    public void shouldSetAppropriateCookies() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", server.port);
        driver.get(location);
        server.waitForEvent();

        Optional<CookieValue> parsedPartyCookieOption = CookieValues.tryParse(driver.manage().getCookieNamed(server.config.getString("divolte.tracking.party_cookie")).getValue());
        assertTrue(parsedPartyCookieOption.isPresent());
        assertThat(
                parsedPartyCookieOption.get(),
                isA(CookieValue.class));

        Optional<CookieValue> parsedSessionCookieOption = CookieValues.tryParse(driver.manage().getCookieNamed(server.config.getString("divolte.tracking.session_cookie")).getValue());
        assertTrue(parsedSessionCookieOption.isPresent());
        assertThat(
                parsedSessionCookieOption.get(),
                isA(CookieValue.class));
    }

    @Test
    public void shouldPickupProvidedPageViewIdFromHash() throws RuntimeException, InterruptedException {
        Preconditions.checkState(null != driver && null != server);
        final String location = String.format("http://127.0.0.1:%d/test-basic-page-provided-pv-id.html", server.port);
        driver.get(location);
        EventPayload event = server.waitForEvent();
        BrowserEventData eventData = event.exchange.getAttachment(EVENT_DATA_KEY);

        assertEquals("supercalifragilisticexpialidocious", eventData.pageViewId);
        assertEquals("supercalifragilisticexpialidocious0", eventData.eventId);
    }


    @Before
    public void setUp() throws Exception {
        final String driverName = System.getenv().getOrDefault(DRIVER_ENV_VAR, PHANTOMJS_DRIVER);

        switch (driverName) {
        case CHROME_DRIVER:
            setupLocalChrome();
            break;
        case SAUCE_DRIVER:
            setupSauceLabs();
            break;
        case BS_DRIVER:
            setupBrowserStack();
            break;
        case PHANTOMJS_DRIVER:
        default:
            driver = new PhantomJSDriver();
            break;
        }

        server = new TestServer("selenium-test-config.conf");
        server.server.run();
    }

    private void setupBrowserStack() throws MalformedURLException {
        final String bsUserName = Optional
                .ofNullable(System.getenv(BS_USER_NAME_ENV_VAR))
                .orElseThrow(() -> new RuntimeException("When using 'browserstack' as Selenium driver, please set the BrowserStack username "
                                                      + "in the " + BS_USER_NAME_ENV_VAR + " env var."));

            final String bsApiKey = Optional
                    .ofNullable(System.getenv(BS_API_KEY_ENV_VAR))
                    .orElseThrow(() -> new RuntimeException("When using 'browserstack' as Selenium driver, please set the BrowserStack username "
                                                          + "in the " + BS_API_KEY_ENV_VAR + " env var."));

            final DesiredCapabilities caps = capabilities.get();
            caps.setCapability("job-name", "Selenium JS test: " + capabilityDescription);
            driver = new RemoteWebDriver(
                    new URL(String.format("http://%s:%s@hub.browserstack.com/wd/hub", bsUserName, bsApiKey)),
                    caps);

    }

    private void setupSauceLabs() throws MalformedURLException {
        final String sauceUserName = Optional
            .ofNullable(System.getenv(SAUCE_USER_NAME_ENV_VAR))
            .orElseThrow(() -> new RuntimeException("When using 'sauce' as Selenium driver, please set the SauceLabs username "
                                                  + "in the " + SAUCE_USER_NAME_ENV_VAR + " env var."));

        final String sauceApiKey = Optional
                .ofNullable(System.getenv(SAUCE_API_KEY_ENV_VAR))
                .orElseThrow(() -> new RuntimeException("When using 'sauce' as Selenium driver, please set the SauceLabs username "
                                                      + "in the " + SAUCE_API_KEY_ENV_VAR + " env var."));
        final String sauceHost = Optional
                .ofNullable(System.getenv(SAUCE_HOST_ENV_VAR))
                .orElse("localhost");

        final int saucePort = Optional
                .ofNullable(System.getenv(SAUCE_PORT_ENV_VAR)).map(Ints::tryParse)
                .orElse(4445);

        final DesiredCapabilities caps = capabilities.get();
        caps.setCapability("job-name", "Selenium JS test: " + capabilityDescription);
        driver = new RemoteWebDriver(
                new URL(String.format("http://%s:%s@%s:%d/wd/hub", sauceUserName, sauceApiKey, sauceHost, saucePort)),
                caps);
    }

    private void setupLocalChrome() {
        System.setProperty("webdriver.chrome.driver",
                Optional.ofNullable(System.getenv(CHROME_DRIVER_LOCATION_ENV_VAR))
                .orElseThrow(
                        () -> new RuntimeException("When using 'chrome' as Selenium driver, please set the location of the "
                                + "Chrome driver manager server thingie in the env var: " + CHROME_DRIVER_LOCATION_ENV_VAR)));
        driver = new ChromeDriver();
    }

    @After
    public void tearDown() {
        if (null != driver) {
            driver.quit();
            driver = null;
        }
        if (null != server) {
            server.server.shutdown();
            server = null;
        }
    }
}
