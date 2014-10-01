package io.divolte.server;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.openqa.selenium.remote.DesiredCapabilities;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public final class SauceLabsBrowserList {
    public static final Iterable<Object[]> THE_LIST = ImmutableList.of(
    // Windows XP
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "6");
                caps.setCapability("deviceName", "");
                return caps;
            }, "IE6 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "30");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Chrome 30 on Windows XP" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows XP");
                caps.setCapability("version", "27");
                caps.setCapability("deviceName", "");
                return caps;
            }, "FF27 on Windows XP" },

            // Windows 7
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "10");
                caps.setCapability("deviceName", "");
                return caps;
            }, "IE10 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.opera();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "12");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Opera 12 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "35");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Chrome 35 on Windows 7" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows 7");
                caps.setCapability("version", "30");
                caps.setCapability("deviceName", "");
                return caps;
            }, "FF30 on Windows 7" },

            // Windows 8
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "10");
                caps.setCapability("deviceName", "");
                return caps;
            }, "IE10 on Windows 8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "35");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Chrome 35 on Windows 8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Windows 8");
                caps.setCapability("version", "30");
                caps.setCapability("deviceName", "");
                return caps;
            }, "FF30 on Windows 8" },

            // Windows 8.1
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.internetExplorer();
                caps.setCapability("platform", "Windows 8.1");
                caps.setCapability("version", "11");
                caps.setCapability("deviceName", "");
                return caps;
            }, "IE11 on Windows 8.1" },

            // OS X
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.6");
                caps.setCapability("version", "5");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Safari 5 on OS X 10.6" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.8");
                caps.setCapability("version", "6");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Safari 6 on OS X 10.8" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.safari();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "7");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Safari 7 on OS X 10.9" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "33");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Chrome 33 on OS X 10.9" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "30");
                caps.setCapability("deviceName", "");
                return caps;
            }, "FF30 on OS X 10.9" },

            // Linux
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.opera();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "12");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Opera 12 on Linux" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.chrome();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "35");
                caps.setCapability("deviceName", "");
                return caps;
            }, "Chrome 35 on Linux" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.firefox();
                caps.setCapability("platform", "Linux");
                caps.setCapability("version", "30");
                caps.setCapability("deviceName", "");
                return caps;
            }, "FF30 on Linux" },

            // iOS
            new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.iphone();
                caps.setCapability("platform", "OS X 10.9");
                caps.setCapability("version", "7.1");
                caps.setCapability("device-orientation", "portrait");
                return caps;
            }, "iOS 7.1 on iPhone" }, new Object[] { (Supplier<DesiredCapabilities>) () -> {
                final DesiredCapabilities caps = DesiredCapabilities.iphone();
                caps.setCapability("platform", "OS X 10.8");
                caps.setCapability("version", "6.1");
                caps.setCapability("device-orientation", "portrait");
                return caps;
            }, "iOS 6.1 on iPhone" });

    public static final String browserNameList() {
        Joiner joiner = Joiner.on('\n');
        return joiner.join(StreamSupport.stream(THE_LIST.spliterator(),  false).map((param) -> param[1]).toArray());
    }
}
