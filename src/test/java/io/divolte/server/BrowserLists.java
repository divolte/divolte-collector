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

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.openqa.selenium.remote.DesiredCapabilities;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public final class BrowserLists {
    public static final Collection<Object[]> SAUCE_BROWSER_LIST = ImmutableList.of(
            // Windows XP
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "6");
                return caps;
            }, "IE6 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "30");
                return caps;
            }, "Chrome 30 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "49");
                return caps;
            }, "Chrome 49 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "27");
                return caps;
            }, "FF27 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "44.0");
                // SauceLabs workaround: without this initializing the Firefox driver fails.
                caps.setCapability("marionette", "false");
                return caps;
            }, "FF44 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "45.0");
                // SauceLabs workaround: without this initializing the Firefox driver fails.
                caps.setCapability("marionette", "false");
                return caps;
            }, "FF45 on Windows XP" },

            // Windows 7
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "10");
                return caps;
            }, "IE10 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                // We really want (ancient) Opera here, not blink.
                @SuppressWarnings("deprecation")
                final DesiredCapabilities caps = DesiredCapabilities.opera();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "12.12");
                return caps;
            }, "Opera 12.12 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "35");
                return caps;
            }, "Chrome 35 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "30");
                return caps;
            }, "FF30 on Windows 7" },

            // Windows 8
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "10");
                return caps;
            }, "IE10 on Windows 8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "35");
                return caps;
            }, "Chrome 35 on Windows 8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "30");
                return caps;
            }, "FF30 on Windows 8" },

            // Windows 8.1
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 8.1");
                caps.setCapability("version", "11");
                return caps;
            }, "IE11 on Windows 8.1" },

            // Windows 10
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.edge();
                caps.setCapability("platform", "Windows 10");
                caps.setCapability("version", "13.10586");
                return caps;
            }, "Edge 13 on Windows 10" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.edge();
                caps.setCapability("platform", "Windows 10");
                caps.setCapability("version", "14.14393");
                return caps;
            }, "Edge 14 on Windows 10" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows 10");
                caps.setCapability("version", "54.0");
                return caps;
            }, "Chrome 54 on Windows 10" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows 10");
                caps.setCapability("version", "50.0");
                return caps;
            }, "FF 50 on Windows 10" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 10");
                caps.setCapability("version", "11.103");
                return caps;
            }, "IE 11 on Windows 10" },

            // OS X
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.8");
                caps.setCapability("version", "6.0");
                return caps;
            }, "Safari 6 on OS X 10.8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "7.0");
                return caps;
            }, "Safari 7 on OS X 10.9" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.10");
                caps.setCapability("version", "8.0");
                return caps;
            }, "Safari 8 on OS X 10.10" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.11");
                caps.setCapability("version", "9.0");
                return caps;
            }, "Safari 9 on OS X 10.11" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.11");
                caps.setCapability("version", "10.0");
                return caps;
            }, "Safari 10 on OS X 10.11" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "macOS 10.12");
                caps.setCapability("version", "10.0");
                return caps;
            }, "Safari 10 on macOS 10.12" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "33");
                return caps;
            }, "Chrome 33 on OS X 10.9" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "macOS 10.12");
                caps.setCapability("version", "54");
                return caps;
            }, "Chrome 54 on macOS 10.12" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "30");
                return caps;
            }, "FF30 on OS X 10.9" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "macOS 10.12");
                caps.setCapability("version", "50");
                return caps;
            }, "FF50 on macOS 10.12" },

            // Linux
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                // We really want (ancient) Opera here, not blink.
                @SuppressWarnings("deprecation")
                final DesiredCapabilities caps = DesiredCapabilities.opera();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "12.15");
                return caps;
            }, "Opera 12.15 on Linux" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "35");
                return caps;
            }, "Chrome 35 on Linux" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "30");
                return caps;
            }, "FF30 on Linux" }
    );

    public static final Collection<Object[]> BS_BROWSER_LIST = ImmutableList.of(
            // Windows XP
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browser", "IE");
                caps.setCapability("browser_version", "11.0");
                caps.setCapability("os", "Windows");
                caps.setCapability("os_version", "8.1");
                caps.setCapability("resolution", "1280x1024");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "IE11 on Windows 8.1" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browser", "IE");
                caps.setCapability("browser_version", "10.0");
                caps.setCapability("os", "Windows");
                caps.setCapability("os_version", "8");
                caps.setCapability("resolution", "1280x1024");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "IE10 on Windows 8" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browser", "Safari");
                caps.setCapability("browser_version", "7.0");
                caps.setCapability("os", "OS X");
                caps.setCapability("os_version", "Mavericks");
                caps.setCapability("resolution", "1280x1024");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "Safari 7 on OS X Mavericks" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browser", "IE");
                caps.setCapability("browser_version", "8.0");
                caps.setCapability("os", "Windows");
                caps.setCapability("os_version", "7");
                caps.setCapability("resolution", "1280x1024");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "IE8 on Windows 7" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browserName", "iPhone");
                caps.setCapability("platform", "MAC");
                caps.setCapability("device", "iPhone 5");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "iOS 6.1 on iPhone 5" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browserName", "iPhone");
                caps.setCapability("platform", "MAC");
                caps.setCapability("device", "iPhone 5C");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "iOS 7.0 on iPhone 5C" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browserName", "android");
                caps.setCapability("platform", "ANDROID");
                caps.setCapability("device", "Samsung Galaxy S III");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "Android 4.1 on Samsung Galaxy S III" },
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = new DesiredCapabilities();
                caps.setCapability("browserName", "android");
                caps.setCapability("platform", "ANDROID");
                caps.setCapability("device", "Amazon Kindle Fire 2");
                caps.setCapability("browserstack.local", "true");
                return caps;
            }, "Android 4 on Amazon Kindle Fire 2" }
    );

    public static String browserNameList(final Iterable<Object[]> list) {
        Joiner joiner = Joiner.on('\n');
        return joiner.join(StreamSupport.stream(list.spliterator(),  false).map((param) -> param[1]).toArray());
    }
}
