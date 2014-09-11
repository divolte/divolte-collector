package io.divolte.browser;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;

import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.thoughtworks.selenium.SeleneseTestCase.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertThat;

public class HeadlessLocalJavaScriptEventTest {

    private Undertow server;
    private BlockingQueue<HttpServerExchange> incoming;

    // Default params
    private final String REFERER_FIELD_PRODUCER = "r";
    private final String LOCATION_FIELD_PRODUCER = "l";
    private final String VIEWPORT_PIXEL_WIDTH = "w";
    private final String VIEWPORT_PIXEL_HEIGHT = "h";
    private final String SCREEN_PIXEL_WIDTH = "i";
    private final String SCREEN_PIXEL_HEIGHT = "j";

    private WebDriver driver;

    @Before
    public void setUp() throws Exception {
        incoming = new ArrayBlockingQueue<>(100);

        // Headless
        driver = new PhantomJSDriver();

        // You're able to manage sattings though manage() method on driver:
        // driver.manage().timeouts().pageLoadTimeout(3, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        driver.quit();
    }

    @Test
    public void shouldSendDefaultEvents() throws InterruptedException {
        startServer((exchange) -> {
            exchange.getResponseSender().send("simple text response.");
            incoming.add(exchange);
        });

        // Open the start page, and check the title
        driver.get("http://localhost:9999/");
        assertEquals("DVT", driver.getTitle());

        // Opening the page triggers an event to devolte
        HttpServerExchange exchange = getLatestExchange();
        Map<String, Deque<String>> params = exchange.getQueryParameters();

        assertThat(exchange.getRequestURL(), is("http://localhost:9999/event"));

        assertThat(params, hasKey(SCREEN_PIXEL_WIDTH));
        assertThat(params, hasKey(SCREEN_PIXEL_HEIGHT));
        assertThat(params, hasKey(VIEWPORT_PIXEL_WIDTH));
        assertThat(params, hasKey(VIEWPORT_PIXEL_HEIGHT));
        assertThat(params, hasKey(LOCATION_FIELD_PRODUCER));
        // Referer field is not send by PhantomJS
        // assertThat(params, hasKey(REFERER_FIELD_PRODUCER));


        // Trigger custom event by entering smy favorite langage.
        WebElement input = driver.findElement(By.id("favoriteLanguage"));
        input.sendKeys("Java" + Keys.ENTER);

        exchange = getLatestExchange();
        assertThat(exchange.getRequestURL(), is("http://localhost:9999/event"));

        params = exchange.getQueryParameters();
        assertThat(params, hasKey("t"));
        assertThat(params, hasKey("t.text"));

        assertThat(params.get("t").getFirst(), is("customInputLanguage"));
        assertThat(params.get("t.text").getFirst(), is("Java"));

        stopServer();
    }

    private HttpServerExchange getLatestExchange() throws InterruptedException {
        return Optional.ofNullable(incoming.poll(10, TimeUnit.SECONDS))
                .orElseThrow(() -> new InterruptedException("Reading response took longer than 10s."));
    }

    private void startServer(HttpHandler eventRequestHandler) {
        final PathHandler handler = new PathHandler();
        handler.addExactPath("/event", eventRequestHandler);
        handler.addPrefixPath("/", createStaticResourceHandler());

        server = Undertow.builder()
                .addHttpListener(9999, "127.0.0.1")
                .setHandler(handler)
                .build();

        server.start();
    }

    private HttpHandler createStaticResourceHandler() {
        final ResourceManager staticResources =
                new ClassPathResourceManager(getClass().getClassLoader(), "static");
        // Cache tuning is copied from Undertow unit tests.
        final ResourceManager cachedResources =
                new CachingResourceManager(100, 65536,
                new DirectBufferCache(1024, 10, 10480),
                staticResources,
                (int) Duration.ofDays(1).getSeconds());
        final ResourceHandler resourceHandler = new ResourceHandler(cachedResources);
        resourceHandler.setWelcomeFiles("index.html");
        return resourceHandler;
    }

    private void stopServer() {
        server.stop();
    }
}
