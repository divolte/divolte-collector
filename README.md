Divolte Collector
=================

This repository contains the source for the Divolte Collector, and its User Guide. This is an
open-source component of [Divolte](http://www.godatadriven.com/divolte.html), a platform for
collecting your own clickstream data to allow custom analytics.

The collector has the following features:

 - Single-line deployment: your HTML pages only need one new line of HTML.
 - Events can be published to HDFS for bulk analytics and/or Kafka for real-time processing.
 - Events are published as Avro records for convenient down-stream processing.
 - Custom Avro schemas supported. Fields can contain information extracted from the URLs on
   your site (amongst other things).

For more information you can browse the current
[User Guide](docs/index.rst).

Prerequisites
-------------

In order to build the Divolte Collector you need to have following installed:

 - [Java 8 SDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
   (or newer). We build and test with Oracle's SDK; other variants should work. (Let us know!)
 - [Sphinx 1.2.x](http://sphinx-doc.org) (or newer). This is only required for building the user
   documentation.

Building
--------

To build the Divolte Collector server itself:

    % ./gradlew zip

or

    % ./gradlew tarball

This will build everything and produce an elementary distribution archive under the
`build/distributions/` directory.

To build the User Guide:

    % ./gradlew userdoc

This will build the documentation and place it under the `build/userdoc/html/` directory.

Testing
-------

Some parts of the server are covered by automated tests. These can be executed with:

    % ./gradlew test

By default this will skip browser-based integration tests. Currently browser-based
testing is supported using:

 - [ChromeDriver](https://sites.google.com/a/chromium.org/chromedriver/)
 - [PhantomJS](http://phantomjs.org)
 - [SauceLabs](http://saucelabs.com)

### Chromedriver ###

ChromeDriver must be installed locally. Under OS X this can be installed via
HomeBrew:

    % brew install chromedriver

Tests can then be executed:

    % SELENIUM_DRIVER=chrome CHROME_DRIVER=/usr/local/Cellar/chromedriver/2.11/bin/chromedriver ./gradlew cleanTest test

### PhantomJS ###

*Note: broken pending a fix that should appear in PhantomJS 2.0.*

PhantomJS must be installed locally. Under OS X this can be installed via
HomeBrew:

    % brew install phantomjs

Tests can then be executed:

    % SELENIUM_DRIVER=phantom gradle cleanTest test

### SauceLabs ###

If you have a SauceLabs account, you can test against a wide variety of browsers.
Once you have a username and API key, tests can then be executed:

    % export SAUCE_USER_NAME=<username>
    % export SAUCE_API_KEY=<api key>
    % SELENIUM_DRIVER=sauce ./gradle cleanTest test

These tests can take quite some time to execute.

Deployment
----------

Please refer to the [User Guide](docs/index.rst)
for information on deploying and configuring the Divolte Collector, as well as information on how to work
with the collected events.

License
-------

The Divolte Collector is licensed under the terms of the Apache License, Version 2.0.
