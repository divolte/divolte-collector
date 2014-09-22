package io.divolte.server;

import static io.divolte.server.BaseEventHandler.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/*
 * Requires PhantomJS:
 * Mac: brew update && brew install phantomjs
 * Windows: make sure phantomjs.exe is on your PATH.
 */
public class SeleniumJavaScriptTest {
    public final static String DRIVER_ENV_VAR = "SELENIUM_DRIVER";
    public final static String CHROME_DRIVER_LOCATION_ENV_VAR = "CHROME_DRIVER";

    public final static String PHANTOMJS_DRIVER = "phantomjs";
    public final static String CHROME_DRIVER = "chrome";

    private final BlockingQueue<EventPayload> incoming = new ArrayBlockingQueue<>(100);

    private WebDriver driver;

    private Server server;
    private final int port = findFreePort();

    @Test
    public void shouldSignalWhenOpeningPage() throws InterruptedException {
        driver.get(String.format("http://localhost:%d/", port));
        assertThat(driver.getTitle(), is("DVT"));

        EventPayload event = waitForEvent();

        assertTrue(event.exchange.getQueryParameters().containsKey(PARTY_ID_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(NEW_PARTY_ID_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(SESSION_ID_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(FIRST_IN_SESSION_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(PAGE_VIEW_ID_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(EVENT_ID_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(EVENT_TYPE_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(CLIENT_TIMESTAMP_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(LOCATION_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(VIEWPORT_PIXEL_WIDTH_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(SCREEN_PIXEL_WIDTH_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(SCREEN_PIXEL_HEIGHT_QUERY_PARAM));
        assertTrue(event.exchange.getQueryParameters().containsKey(DEVICE_PIXEL_RATIO));
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

        Config config = ConfigFactory.parseResources("selenium-test-config.conf")
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
