package io.divolte.server;

import static io.divolte.server.BaseEventHandler.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import io.divolte.server.CookieValues.CookieValue;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SeleniumJavaScriptTest {
    public final static String DRIVER_ENV_VAR = "SELENIUM_DRIVER";
    public final static String CHROME_DRIVER_LOCATION_ENV_VAR = "CHROME_DRIVER";

    public final static String PHANTOMJS_DRIVER = "phantomjs";
    public final static String CHROME_DRIVER = "chrome";

    private final BlockingQueue<EventPayload> incoming = new ArrayBlockingQueue<>(100);

    private WebDriver driver;

    private Config config;
    private Server server;
    private final int port = findFreePort();

    @Test
    public void shouldSignalWhenOpeningPage() throws InterruptedException {
        final long requestStartTime = System.currentTimeMillis();
        final long ONE_DAY = 12L * 3600L * 1000L;

        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", port);
        driver.get(location);

        EventPayload viewEvent = waitForEvent();

        final Map<String, Deque<String>> params = viewEvent.exchange.getQueryParameters();
        assertTrue(params.containsKey(PARTY_ID_QUERY_PARAM));

        assertTrue(params.containsKey(NEW_PARTY_ID_QUERY_PARAM));
        assertEquals("t", params.get(NEW_PARTY_ID_QUERY_PARAM).getFirst());

        assertTrue(params.containsKey(SESSION_ID_QUERY_PARAM));
        assertTrue(params.containsKey(FIRST_IN_SESSION_QUERY_PARAM));
        assertEquals("t", params.get(FIRST_IN_SESSION_QUERY_PARAM).getFirst());

        assertTrue(params.containsKey(PAGE_VIEW_ID_QUERY_PARAM));
        assertTrue(params.containsKey(EVENT_ID_QUERY_PARAM));

        assertTrue(params.containsKey(EVENT_TYPE_QUERY_PARAM));
        assertEquals("pageView", params.get(EVENT_TYPE_QUERY_PARAM).getFirst());

        assertTrue(params.containsKey(LOCATION_QUERY_PARAM));
        assertEquals(location, params.get(LOCATION_QUERY_PARAM).getFirst());

        /*
         * We don't really know anything about the clock on the executing browser,
         * but we'd expect it to be a reasonably accurate clock on the same planet.
         * So, if it is within +/- 12 hours of our clock, we think it's fine.
         */
        assertTrue(params.containsKey(CLIENT_TIMESTAMP_QUERY_PARAM));
        assertThat(
                Long.parseLong(params.get(CLIENT_TIMESTAMP_QUERY_PARAM).getFirst(), 36),
                allOf(greaterThan(requestStartTime - ONE_DAY), lessThan(requestStartTime + ONE_DAY)));

        /*
         * We could set the window size through Selenium, but it's unsure
         * whether that's meaningful on mobile devices, such as phones and tables.
         */
        final Dimension windowSize = driver.manage().window().getSize();
        assertTrue(params.containsKey(VIEWPORT_PIXEL_WIDTH_QUERY_PARAM));
        assertThat(
                Integer.parseInt(params.get(VIEWPORT_PIXEL_WIDTH_QUERY_PARAM).getFirst(), 36),
                lessThanOrEqualTo(windowSize.width));

        assertTrue(params.containsKey(VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM));
        assertThat(
                Integer.parseInt(params.get(VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM).getFirst(), 36),
                lessThan(windowSize.height));

        assertTrue(params.containsKey(SCREEN_PIXEL_WIDTH_QUERY_PARAM));
        assertThat(
                Integer.parseInt(params.get(SCREEN_PIXEL_WIDTH_QUERY_PARAM).getFirst(), 36),
                greaterThanOrEqualTo(windowSize.width));

        assertTrue(params.containsKey(SCREEN_PIXEL_HEIGHT_QUERY_PARAM));
        assertThat(
                Integer.parseInt(params.get(SCREEN_PIXEL_HEIGHT_QUERY_PARAM).getFirst(), 36),
                greaterThanOrEqualTo(windowSize.height));

        assertTrue(params.containsKey(DEVICE_PIXEL_RATIO));
    }

    @Test
    public void shouldSendCustomEvent() throws RuntimeException, InterruptedException {
        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", port);
        driver.get(location);
        waitForEvent();

        driver.findElement(By.id("custom")).click();
        EventPayload viewEvent = waitForEvent();

        final Map<String, Deque<String>> params = viewEvent.exchange.getQueryParameters();
        assertTrue(params.containsKey(EVENT_TYPE_QUERY_PARAM));
        assertEquals("custom", params.get(EVENT_TYPE_QUERY_PARAM).getFirst());

        assertTrue(params.containsKey(EVENT_TYPE_QUERY_PARAM + ".key"));
        assertEquals("value", params.get(EVENT_TYPE_QUERY_PARAM + ".key").getFirst());
    }

    @Test
    public void shouldSetAppropriateCookies() throws RuntimeException, InterruptedException {
        final String location = String.format("http://127.0.0.1:%d/test-basic-page.html", port);
        driver.get(location);
        waitForEvent();

        Optional<CookieValue> parsedPartyCookieOption = CookieValues.tryParse(driver.manage().getCookieNamed(config.getString("divolte.tracking.party_cookie")).getValue());
        assertTrue(parsedPartyCookieOption.isPresent());
        assertThat(
                parsedPartyCookieOption.get(),
                isA(CookieValue.class));

        Optional<CookieValue> parsedSessionCookieOption = CookieValues.tryParse(driver.manage().getCookieNamed(config.getString("divolte.tracking.session_cookie")).getValue());
        assertTrue(parsedSessionCookieOption.isPresent());
        assertThat(
                parsedSessionCookieOption.get(),
                isA(CookieValue.class));
    }


    @Before
    public void setUp() throws Exception {
        final String driverName = System.getenv().getOrDefault(DRIVER_ENV_VAR, PHANTOMJS_DRIVER);

        switch (driverName) {
        case CHROME_DRIVER:
            System.setProperty("webdriver.chrome.driver",
                    Optional.ofNullable(System.getenv(CHROME_DRIVER_LOCATION_ENV_VAR))
                    .orElseThrow(
                            () -> new RuntimeException("When using 'chrome' as Selenium driver, please set the location of the "
                                    + "Chrome driver manager server thingie in the env var: " + CHROME_DRIVER_LOCATION_ENV_VAR)));
            driver = new ChromeDriver();
            break;
        case PHANTOMJS_DRIVER:
        default:
            driver = new PhantomJSDriver();
            break;
        }

        config = ConfigFactory.parseResources("selenium-test-config.conf")
                .withFallback(ConfigFactory.parseString("divolte.server.port = " + port));
        server = new Server(config, (exchange, buffer, record) -> incoming.add(new EventPayload(exchange, buffer, record)));
        server.run();
    }

    @After
    public void tearDown() {
        driver.quit();
        server.shutdown();
    }

    private EventPayload waitForEvent() throws RuntimeException, InterruptedException {
        return Optional.ofNullable(incoming.poll(10, TimeUnit.SECONDS)).orElseThrow(() -> new RuntimeException("Timed out while waiting for server side event to occur."));
    }

    private static final class EventPayload {
        final HttpServerExchange exchange;
        final AvroRecordBuffer buffer;
        final GenericRecord record;

        private EventPayload(HttpServerExchange exchange, AvroRecordBuffer buffer, GenericRecord record) {
            this.exchange = exchange;
            this.buffer = buffer;
            this.record = record;
        }
    }

    /*
     * Theoretically, this is prone to race conditions,
     * but in practice, it should be fine due to the way
     * TCP stacks allocate port numbers (i.e. increment
     * for the next one).
     */
    private static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        return -1;
    }
}
