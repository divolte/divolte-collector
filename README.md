Divolte Collector
=================

### *Scalable clickstream collection for Hadoop and Kafka* ###

Divolte Collector is a scalable and performant server for collecting clickstream data in HDFS and on Kafka topics. It uses a JavaScript tag on the client side to gather user interaction data, similar to many other web tracking solutions. Divolte Collector can be used as the foundation to build anything from basic web analytics dashboarding to real-time recommender engines or banner optimization systems.

[http://divolte.io](http://divolte.io)

![Divolte Collector](http://divolte-website.s3-website-eu-west-1.amazonaws.com/images/architecture.png)

Online documentation and downloads
----------------------------------

You can find the latest downloads and documentation on our [project website](http://divolte.io). There is a series of examples for working with collected data in Spark, Hive / Impala, and Kafka in this repository: [https://github.com/divolte/divolte-examples](https://github.com/divolte/divolte-examples).

Features
--------

- **Single tag site integration**: Including Divolte Collector is a HTML one-liner. Just load the JavaScript at the end of your document body.
- **Built for Hadoop and Kafka**: All data is collected directly in HDFS and on Kafka queues. Divolte Collector is both a HDFS client and a Kafka producer. No ETL or intermediate storage.
- **Structured data collection**: All data is captured in Apache Avro records using your own schema definition. Divolte Collector does not enforce a particular structure on your data.
- **User agent parsing**: It's not just a string. Add rich user-agent information to your click event records on the fly.
- **ip2geo lookup**: Attach geo-coordinates to requests on the fly. (This requires a third-party database; a free version is available.)
- **Fast**: Handle many thousands of requests per second on a single node. Scale out as you need.
- **Custom events**: Just like any web analytics solution, you can log any event. Supply custom parameters in your page or JavaScript and map them onto your Avro schema.
- **Integrate with anything**: Work with anything that understands Avro and either HDFS or Kafka. Hive, Impala, Spark, Spark Streaming, Storm, etc. No log file parsing is required.
- **Open source**: Divolte Collector is hosted on GitHub and released under the Apache License, Version 2.0.

Building Prerequisites
----------------------

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

Unit tests can be executed with:

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

    % SELENIUM_DRIVER=chrome CHROME_DRIVER=/usr/local/Cellar/chromedriver/2.26/bin/chromedriver ./gradlew cleanTest test

### PhantomJS ###

PhantomJS must be installed locally. Under OS X this can be installed via
HomeBrew:

    % brew install phantomjs

Tests can then be executed:

    % SELENIUM_DRIVER=phantom ./gradlew cleanTest test

(Some tests currently fail.)

### SauceLabs ###

If you have a SauceLabs account, you can test against a wide variety of browsers.
Once you have a username and API key and
[Sauce Connect](https://docs.saucelabs.com/reference/sauce-connect/) running, tests
can then be executed:

    % export SAUCE_USER_NAME=<username>
    % export SAUCE_API_KEY=<api key>
    % SELENIUM_DRIVER=sauce ./gradlew cleanTest test

These tests can take quite some time to execute. Not all succeed.

License
-------

The Divolte Collector is licensed under the terms of the Apache License, Version 2.0.
