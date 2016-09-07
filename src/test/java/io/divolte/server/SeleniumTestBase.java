/*
 * Copyright 2016 GoDataDriven B.V.
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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.divolte.server.ServerTestUtils.TestServer;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.divolte.server.BrowserLists.*;

@RunWith(Parameterized.class)
public abstract class SeleniumTestBase {

    public static final String DRIVER_ENV_VAR = "SELENIUM_DRIVER";
    public static final String PHANTOMJS_DRIVER = "phantomjs";
    public static final String CHROME_DRIVER = "chrome";
    public static final String SAUCE_DRIVER = "sauce";
    public static final String BS_DRIVER = "browserstack";

    public static final String SAUCE_USER_NAME_ENV_VAR = "SAUCE_USER_NAME";
    public static final String SAUCE_API_KEY_ENV_VAR = "SAUCE_API_KEY";
    public static final String SAUCE_HOST_ENV_VAR = "SAUCE_HOST";
    public static final String SAUCE_PORT_ENV_VAR = "SAUCE_PORT";

    public static final String BS_USER_NAME_ENV_VAR = "BS_USER_NAME";
    public static final String BS_API_KEY_ENV_VAR = "BS_API_KEY";

    public static final String CHROME_DRIVER_LOCATION_ENV_VAR = "CHROME_DRIVER";

    public static final DesiredCapabilities LOCAL_RUN_CAPABILITIES;
    static {
        LOCAL_RUN_CAPABILITIES = new DesiredCapabilities();
        LOCAL_RUN_CAPABILITIES.setBrowserName("Local Selenium instructed browser");
    }

    @Rule
    public final TestRule suppressWebDriverNavigationExceptions = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            try {
                base.evaluate();
            } catch (final WebDriverException e) {
                if (e.getMessage().contains("history navigation does not work")) {
                    throw new AssumptionViolatedException("Selenium driver doesn't support navigation required for this test.", e);
                }
                throw e;
            }
        }
    };

    @Nullable
    protected WebDriver driver;
    @Nullable
    protected TestServer server;
    @Parameter(0)
    public Supplier<DesiredCapabilities> capabilities;
    @Parameter(1)
    public String capabilityDescription;
    @Parameter(2)
    public boolean quirksMode;

    @Parameters(name = "Selenium JS test: {1} (quirks-mode={2})")
    public static Iterable<Object[]> sauceLabBrowsersToTest() {
        final Collection<Object[]> browserList;
        if (!System.getenv().containsKey(DRIVER_ENV_VAR)) {
            browserList = Collections.emptyList();
        } else if (SAUCE_DRIVER.equals(System.getenv().get(DRIVER_ENV_VAR))) {
            browserList = SAUCE_BROWSER_LIST;
            System.out.println("Selenium test running on SauceLabs with these browsers:\n" + browserNameList(SAUCE_BROWSER_LIST));
        } else if (BS_DRIVER.equals(System.getenv().get(DRIVER_ENV_VAR))) {
            browserList = BS_BROWSER_LIST;
            System.out.println("Selenium test running on BrowserStack with these browsers:\n" + browserNameList(BS_BROWSER_LIST));
        } else {
            // Parameters are not used for non-sauce tests
            browserList = ImmutableList.of(new Object[] {
                    (Supplier<DesiredCapabilities>) () -> LOCAL_RUN_CAPABILITIES, "Local JS test run"
            });
        }
        // For each browser, we need to run in and out of quirks mode.
        return browserList.stream()
                .flatMap((browser) ->
                        ImmutableList.of(new Object[] { browser[0], browser[1], false },
                                         new Object[] { browser[0], browser[1], true  }).stream())
                .collect(Collectors.toList());
    }

    public enum TEST_PAGES {
            BASIC("test-basic-page"),
            BASIC_COPY("test-basic-page-copy"),
            PAGE_VIEW_SUPPLIED("test-basic-page-provided-pv-id"),
            CUSTOM_JAVASCRIPT_NAME("test-custom-javascript-name"),
            CUSTOM_PAGE_VIEW("test-custom-page-view");

            private final String resourceName;

            TEST_PAGES(final String resourceName) {
                this.resourceName = Objects.requireNonNull(resourceName);
            }
        }

    protected String urlOf(final TEST_PAGES page) {
        final String modeString = quirksMode ? "quirks" : "strict";
        return String.format("http://127.0.0.1:%d/%s/%s.html",
                             server.port, modeString, page.resourceName);
    }

    protected void doSetUp(final String configFileName) throws Exception {
        doSetUp(Optional.of(configFileName));
    }

    protected void doSetUp() throws Exception {
        doSetUp(Optional.empty());
    }

    private void doSetUp(final Optional<String> configFileName) throws Exception {
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

        server = configFileName.map(TestServer::new).orElseGet(TestServer::new);
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
