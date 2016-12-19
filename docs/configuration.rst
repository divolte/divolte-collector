*************
Configuration
*************
This chapter describes the configuration mechanisms and available options for Divolte Collector. If you are completely new to Divolte Collector, it is probably a good idea to go through the :doc:`getting_started` guide first.

Configuration files
===================
The main configuration for Divolte Collector consists of three files:

- :file:`divolte-env.sh`: a shell script that is included in the startup script to set environment variables and JVM startup arguments.
- :file:`logback.xml`: the logging configuration.
- :file:`divolte-collector.conf`: the main configuration file for Divolte Collector.

Configuration directory
-----------------------
Divolte Collector will try to find configuration files at startup in the configuration directory. Typically this is the :file:`conf/` directory nested under the Divolte Collector installation. Divolte Collector will try to locate the configuration directory at :file:`../conf` relative to the startup script. The configuration directory can be overridden by setting the :envvar:`DIVOLTE_CONF_DIR` environment variable. If set, the value will be used as configuration directory. If you have installed Divolte Collector from a RPM, the init script will set this variable to :file:`/etc/divolte-collector`.

divolte-env.sh
--------------
This shell script is run by the startup script prior to starting the Divolte Collector JVM. The following environment variables influence Divolte Collector.

.. envvar:: HADOOP_CONF_DIR

:Description:
  Directory where Hadoop/HDFS configuration files are to be found. This directory is included in the classpath on startup, which causes the HDFS client to load the configuration files.

:Example:

  .. code-block:: bash

    HADOOP_CONF_DIR=/etc/hadoop/conf

.. envvar:: JAVA_HOME

:Description:
  The directory where the JRE/JDK is located. Divolte Collector will use :command:`$JAVA_HOME/bin/java` as the Java executable during startup. If this is not set, Divolte Collector will attempt to find a suitable JDK in a number of common Java installation locations on Linux systems. It is however not recommended to rely on this mechanism for production use.

:Example:

  .. code-block:: bash

    JAVA_HOME=/usr/java/default

.. envvar:: DIVOLTE_JAVA_OPTS

:Description:
  Additional arguments passed to the Java Virtual Machine on startup. If not set, by default Divolte Collector will start the JVM with :code:`-XX:+UseG1GC -Djava.awt.headless=true`. It is recommended to use the G1 garbage collector. For light and medium traffic, the defaults tend to work fine. *If this setting is set, Divolte Collector will not add any arguments by itself; this setting overrides the defaults.*

:Example:

  .. code-block:: bash

    DIVOLTE_JAVA_OPTS="-XX:+UseG1GC -Djava.awt.headless=true -XX:+HeapDumpOnOutOfMemoryError"

logback.xml
-----------
Divolte Collector uses the `Logback Project <http://logback.qos.ch>`_ as its logging provider. This provider is configured through the :file:`logback.xml` file in the configuration directory. For more information about the settings in this file, review the `Configuration chapter in the Logback Manual <http://logback.qos.ch/manual/configuration.html>`_.

divolte-collector.conf
----------------------
This is the main configuration file for Divolte Collector. For configuration, Divolte Collector uses the `Typesafe Config library <https://github.com/typesafehub/config>`_. The dialect of the configuration file is a JSON superset called HOCON (for *Human-Optimized Config Object Notation*). HOCON has a nested structure, like JSON, but is slightly less verbose and doesn't require escaping and quoting of strings in many cases. Here we outline some basic features of HOCON.

Nesting and dot separated namespacing can be used interchangeably:

.. code-block:: none

  // This:
  divolte {
    global {
      server {
        host = 127.0.0.1
      }
    }
  }

  // Is the same as this:
  divolte.global.server.host = 127.0.0.1

Environment variable overrides can be used. In this example the ``divolte.global.server.port`` setting defaults to 8290, unless the :envvar:`DIVOLTE_PORT` environment variable is set:

.. code-block:: none

  divolte {
    global {
      server {
        port = 8290
        port = ${?DIVOLTE_PORT}
      }
    }
  }

Objects are merged:

.. code-block:: none

  // This configuration
  divolte {
    global {
      server {
        host = 0.0.0.0
      }
    }
  }

  divolte.global.server {
    port = 8290
  }

  // Will result in this:
  divolte.global.server.host = 0.0.0.0
  divolte.global.server.port = 8290

For a full overview please refer to the `HOCON features and specification <https://github.com/typesafehub/config/blob/master/HOCON.md>`_.

.. warning::

  Be careful when enclosing values in quotes. Quotes are optional, but if present they must be JSON-style double-quotes (``"``). This can easily lead to confusion:

  .. code-block:: none

    // This ...
    divolte.sources.browser.cookie_domain = '.example.com'
    // ... is really equivalent to:
    divolte.sources.browser.cookie_domain = "'.example.com'"

Configuration reference
=======================

The main configuration is read from :file:`divolte-collector.conf`, which consists of several sections:

- *Global* (``divolte.global``): Global settings that affect the entire service.
- *Sources* (``divolte.sources``): Configured sources for Divolte Collector events.
- *Mappings* (``divolte.mappings``): Configured mappings between sources and sinks.
- *Sinks* (``divolte.sinks``): Configured sinks, where Avro events are written.

This documentation uses the path notation for configuration options (e.g. ``divolte.global.server``) but in examples the path and nested notations are used interchangeably.

Global Settings (``divolte.global``)
------------------------------------

This section contains settings which are global in nature. All settings have default values.

HTTP Server Settings (``divolte.global.server``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls the settings for the internal HTTP server of Divolte Collector.

Property: ``divolte.global.server.host``
""""""""""""""""""""""""""""""""""""""""
:Description:
  The address to which the server binds. Set to a specific IP address to selectively listen on that interface, or ``0.0.0.0`` to listen on all interfaces.
:Default:
  The address of a loopback interface.
:Example:

  .. code-block:: none

    divolte.global.server {
      host = 0.0.0.0
    }

Property: ``divolte.global.server.port``
""""""""""""""""""""""""""""""""""""""""
:Description:
  The TCP port on which the server listens.
:Default:
  ``8290``, or the content of the :envvar:`DIVOLTE_PORT` environment variable if set.
:Example:

  .. code-block:: none

    divolte.global.server {
      port = 8290
    }

Property: ``divolte.global.server.use_x_forwarded_for``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Whether to use the :mailheader:`X-Forwarded-For` HTTP header for determining the source IP of a request, if present. If multiple values are present, the last value is used.

  Both of these examples would yield a source IP of ``11.34.82.30``:

  1. | ``X-Forwarded-For: 10.200.13.28, 11.45.82.30``
  2. | ``X-Forwarded-For: 10.200.13.28``
     | ``X-Forwarded-For: 11.45.82.30``

:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.global.server {
      use_x_forwarded_for = true
    }

Property: ``divolte.global.server.serve_static_resources``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When true Divolte Collector serves a static test page at ``/``.
:Default:
  :code:`true`
:Example:

  .. code-block:: none

    divolte.global.server {
      serve_static_resources = false
    }

Global Mapper Settings (``divolte.global.mapper``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls global settings related to the processing of incoming requests after they have been received by the server. Incoming requests for Divolte Collector are responded to as quickly as possible, with mapping and flushing occurring in the background.

Property: ``divolte.global.mapper.threads``
"""""""""""""""""""""""""""""""""""""""""""
:Description:
  The total number of threads that mappers will use to process events. This is a global total; all mappings share the same threads.
:Default:
  1
:Example:

  .. code-block:: none

    divolte.global.mapper {
      threads = 4
    }

Property: ``divolte.global.mapper.buffer_size``
"""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of incoming events, rounded up to the nearest power of 2, to queue for processing *per mapper thread* before starting to drop incoming events. While this buffer is full new events are dropped and a warning is logged. (Dropped requests are not reported to the client: Divolte Collector always responds to clients immediately once minimal validation has taken place.)
:Default:
  1048576
:Example:

  .. code-block:: none

    divolte.global.mapper {
      buffer_size = 10M
    }

Property: ``divolte.global.mapper.duplicate_memory_size``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Clients will sometimes deliver an event multiple times, normally within a short period of time. Divolte Collector contains a probabilistic filter which can detect this, trading off memory for improved results. This setting configures the size of the filter *per mapper thread*, and is multiplied by 8 to yield the actual memory usage.
:Default:
  1000000
:Example:

  .. code-block:: none

    divolte.global.mapper {
      duplicate_memory_size = 10000000
    }

Property: ``divolte.global.mapper.ip2geo_database``
"""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  This configures the ip2geo database for geographic lookups. An ip2geo database can be obtained from `MaxMind <https://www.maxmind.com/en/geoip2-databases>`_. (Free 'lite' versions and commercial versions are available.)

  By default no database is configured. When this setting is absent no attempt will be made to lookup geographic-coordinates for IP addresses. When configured, Divolte Collector monitors the database file and will reload it automatically if it changes.
:Default:
  *Not set*
:Example:

  .. code-block:: none

    divolte.global.mapper {
      ip2geo_database = "/etc/divolte/ip2geo/GeoLite2-City.mmdb"
    }

Property: ``divolte.global.mapper.user_agent_parser``
"""""""""""""""""""""""""""""""""""""""""""""""""""""
This section controls the user agent parsing settings. The user agent parsing is based on an `open source parsing library <https://github.com/before/uadetector>`_ and supports dynamic reloading of the backing database if an internet connection is available.

Property: ``divolte.global.mapper.user_agent_parser.type``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  This setting controls the updating behavior of the user agent parser.

  Possible values are:

  - ``non_updating``:         Uses a local database, bundled with Divolte Collector.
  - ``online_updating``:      Uses a online database only, never falls back to the local database.
  - ``caching_and_updating``: Uses a cached version of the online database and periodically checks for new version at the remote location. Updates are downloaded automatically and cached locally.

  **Important: due to a change in the licensing of the user agent database, the online database for the user agent parser is no longer available.**
:Default:
  ``non_updating``
:Example:

  .. code-block:: none

    divolte.global.mapper.user_agent_parser {
      type = caching_and_updating
    }

Property: ``divolte.global.mapper.user_agent_parser.cache_size``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  User agent parsing is a relatively expensive operation that requires many regular expression evaluations. Very often the same user agent will make consecutive requests and many clients will have the exact same user agent as well. It therefore makes sense to cache the parsing results for re-use in subsequent requests. This setting determines how many unique user agent strings will be cached.
:Default:
  1000
:Example:

  .. code-block:: none

    divolte.global.mapper.user_agent_parser {
      cache_size = 10000
    }

Global HDFS Settings (``divolte.global.hdfs``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls global HDFS settings shared by all HDFS sinks.

Property: ``divolte.global.hdfs.enabled``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  Whether or not HDFS support is enabled or not. If disabled all HDFS sinks are ignored.
:Default:
  :code:`true`
:Example:

  .. code-block:: none

    divolte.global.hdfs {
      enabled = false
    }

Property: ``divolte.global.hdfs.threads``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  Number of threads to use per HDFS sink for writing events. Each thread creates its own files on HDFS.
:Default:
  2
:Example:

  .. code-block:: none

    divolte.global.hdfs {
      threads = 1
    }

Property: ``divolte.global.hdfs.buffer_size``
"""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of mapped events to queue internally *per sink thread* for HDFS before starting to drop them. This value will be rounded up to the nearest power of 2.
:Default:
  1048576
:Example:

  .. code-block:: none

    divolte.global.hdfs.buffer_size {
      max_write_queue = 10M
    }

Property: ``divolte.global.hdfs.client``
""""""""""""""""""""""""""""""""""""""""
:Description:
  Properties that will be used to configure the HDFS client used by HDFS sinks. If set, these properties will be used *instead of* the settings from :file:`hdfs-site.xml` in the directory specified by the :envvar:`HADOOP_CONF_DIR`. Although it is possible to configure all settings here instead of in :envvar:`HADOOP_CONF_DIR` this is not recommended.
:Default:
  *Not set*
:Example:

  .. code-block:: none

    divolte.global.hdfs.client {
      fs.defaultFS = "file:///var/log/divolte/"
    }

Global Kafka Settings (``divolte.global.kafka``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls global Kafka settings shared by all Kafka sinks. At present Divolte Collector only supports connecting to a single Kafka cluster.

Property: ``divolte.global.kafka.enabled``
""""""""""""""""""""""""""""""""""""""""""
:Description:
  This controls whether flushing to Kafka is enabled or not. If disabled all Kafka sinks are ignored. (This is disabled by default because the producer configuration for Kafka is normally site-specific.)
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.global.kafka {
      enabled = true
    }

Property: ``divolte.global.kafka.threads``
""""""""""""""""""""""""""""""""""""""""""
:Description:
  Number of threads to use per Kafka sink for flushing events to Kafka.
:Default:
  2
:Example:

  .. code-block:: none

    divolte.global.kafka {
      threads = 1
    }

Property: ``divolte.global.kafka.buffer_size``
""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of mapped events to queue internally *per sink thread* for Kafka before starting to drop them. This value will be rounded up to the nearest power of 2.
:Default:
  1048576
:Example:

  .. code-block:: none

    divolte.global.kafka.buffer_size {
      max_write_queue = 10M
    }

Property: ``divolte.global.kafka.producer``
"""""""""""""""""""""""""""""""""""""""""""
:Description:
  The configuration to use for Kafka producers. All settings are used as-is to configure the Kafka producer; refer to the `Kafka Documentation <http://kafka.apache.org/082/documentation.html#newproducerconfigs>`_ for further details.
:Default:

  .. code-block:: none

    {
      bootstrap.servers = ["localhost:9092"]
      bootstrap.servers = ${?DIVOLTE_KAFKA_BROKER_LIST}
      client.id = divolte.collector
      client.id = ${?DIVOLTE_KAFKA_CLIENT_ID}

      acks = 1
      retries = 0
      compression.type = lz4
      max.in.flight.requests.per.connection = 1
    }

  Note the use of :envvar:`DIVOLTE_KAFKA_BROKER_LIST` and :envvar:`DIVOLTE_KAFKA_CLIENT_ID` environment variables, if they have been set.

:Example:

  .. code-block:: none

    divolte.global.kafka.producer = {
      metadata.broker.list = ["broker1:9092", "broker2:9092", "broker3:9092"]
      client.id = divolte.collector

      acks = 0
      retries = 5
    }

Sources (``divolte.sources``)
-----------------------------

Sources are endpoints that can receive events. Each source has a name used to identify it when configuring a mapper that uses the source. A source cannot have the same name as a sink (and vice versa). Sources are configured in sections using their name as the configuration path. (Due to the `HOCON merging rules <https://github.com/typesafehub/config/blob/master/HOCON.md#duplicate-keys-and-object-merging>`_, it's not possible to configure multiple sources with the same name.)

Each source has a type configured via a mandatory ``type`` property. Two types of source are supported:

1. ``browser``: An event source for collecting events from users' Web browser activity.
2. ``json``: A low-level event source for collecting events from server or mobile applications.

For example:

.. code-block:: none

  divolte.sources {
    // The name of the source is 'web_source'
    web_source = {
      // This is a browser source.
      type = browser
    }

    app_source = {
      // This is a JSON source.
      type = json
    }
  }

Implicit default source
^^^^^^^^^^^^^^^^^^^^^^^

If no sources are specified a single implicit browser source is created that is equivalent to:

.. code-block:: none

  divolte.sources {
    // The name of the implicit source is 'browser'
    browser = {
      type = browser
    }
  }

If *any* sources are configured this implicit source is not present and all sources must be explicitly specified.

Browser Sources
^^^^^^^^^^^^^^^

A browser source is intended to receive tracking events from a browser. Each browser source serves up a tracking tag (JavaScript). This tag must be integrated into a website for Divolte Collector to receive tracking events. Each page of a website needs to include this:

.. code-block:: html

  <script src="//track.example.com/divolte.js" defer async></script>

The URL will need to use the domain name where you are hosting Divolte Collector, and ``divolte.js`` needs to match the ``javascript.name`` setting of the browser source.

By default loading the tag will trigger a ``pageView`` event. The tag also provides an API for issuing custom
events:

.. code-block:: html

  <script>
    divolte.signal('eventType', { 'foo': 'divolte', 'bar': 42 })
  </script>

The first argument to the :samp:`divolte.signal({...})` function is the type of event, while the second argument is an arbitrary object containing custom parameters associated with the event. Storing the event and its parameters into the configured Avro schema is controlled via mapping; see the :doc:`mapping_reference` chapter for details.

Browser sources are able to detect some cases of corruption in the event data. The most common source of this is due to URLs being truncated, but there are also other sources of corruption between the client and the server. Corrupted events are flagged as such but still made available for mapping. (Mappings may choose to discard corrupted events, but by default they are processed normally.)

Within the namespace for a browser source properties are used to configure it.

Browser source property: ``prefix``
"""""""""""""""""""""""""""""""""""
:Description:
  The path prefix under which the tracking tag is available. Each browser source must have a unique prefix. A trailing slash (``/``) is automatically appended if not specified.
:Default:
  ``/``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      prefix = /tracking
    }

  In this case the tracking tag could be included using:

  .. code-block:: html

    <script src="//track.example.com/tracking/divolte.js" defer async></script>

Browser source property: ``event_suffix``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  The path suffix that will be added to the prefix to determine the complete path that the tracking tag should use for submitting events. Configuring this should not normally be necessary.
:Default:
    ``csc-event``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      event_suffix = web-event
    }

  In this case the tracking tag will submit events using ``/web-event`` as the URL path.

Browser source property: ``party_cookie``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  The name of the cookie used for setting a party identifier.
:Default:
  ``_dvp``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      party_cookie = _pid
    }

Browser source property: ``party_timeout``
""""""""""""""""""""""""""""""""""""""""""
:Description:
  The expiry timeout for the party identifier. If no events occur for this duration, the party identifier is discarded by the browser. Any subsequent events will be cause a new party identifier to be assigned to the browser.
:Default:
  730 days
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      party_timeout = 1000 days
    }

Browser source property: ``session_cookie``
"""""""""""""""""""""""""""""""""""""""""""
:Description:
  The name of the cookie used for tracking the session identifier.
:Default:
  ``_dvs``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      session_cookie = _sid
    }

Browser source property: ``session_timeout``
""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The expiry timeout for a session. A session lapses if no events occur for this duration.
:Default:
  30 minutes
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      session_timeout = 1 hour
    }

Browser source property: ``cookie_domain``
""""""""""""""""""""""""""""""""""""""""""
:Description:
  The cookie domain that is assigned to the cookies. When left empty, the cookies will have no domain explicitly associated with them, which effectively sets it to the website domain of the page that loaded the tag.
:Default:
  *Empty*
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      cookie_domain = ".example.com"
    }

Browser source property: ``javascript.name``
""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The name of the JavaScript loaded as the tag. This is appended to the value of the ``prefix`` property to form the complete path of the tag in the URL.
:Default:
  ``divolte.js``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      javascript.name = tracking.js
    }

  In this case the tracking tag could be included using:

  .. code-block:: html

    <script src="//track.example.com/tracking.js" defer async></script>

Browser source property: ``javascript.logging``
"""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Enable or disable the logging to the JavaScript console in the browser.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      javascript.logging = true
    }

Browser source property: ``javascript.debug``
"""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When enabled, the served JavaScript will be less compact and *slightly* easier to debug. This setting is mainly intended to help track down problems in either the minification process used to reduce the size of the tracking script, or in the behaviour of specific browser versions.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      javascript.debug = true
    }

Browser source property: ``javascript.auto_page_view_event``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When enabled the JavaScript tag automatically generates a ``pageView`` event when loaded, simplifying site integration. If sites wish to control all events (including the initial ``pageView`` event) this can be disabled.
:Default:
  :code:`true`
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      javascript.auto_page_view_event = false
    }

JSON Sources
^^^^^^^^^^^^

A JSON source is intended for receiving tracking events from mobile and server applications. Events are encoded using JSON and submitted using a ``POST`` request. A HTTP request submitting an event must:

- Use the ``POST`` HTTP method. All other methods are forbidden.
- Include a :mailheader:`Content-Type` header specifying the body content is :mimetype:`application/json`. Any other content type will not be accepted.
- Have a ``p`` query parameter containing the party identifier associated with the event.

In addition a :mailheader:`Content-Length` header should be provided.

The request body must contain a JSON object with the following properties:

- ``session_id``: A string that contains the identifier of the session with which this event is associated.
- ``event_id``: A string that contains a unique identifier for this event.
- ``is_new_party``: A boolean that should be :code:`true` if this is the first event for the supplied party identifier, or :code:`false` otherwise.
- ``is_new_session``: A boolean that should be :code:`true` if this is the first event within the session, or :code:`false` otherwise.
- ``client_timestamp_iso``: The time the event was generated according to the client, specified as a string in the `ISO-8601 extended format <https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations>`_. Separators, including the ``T``, are mandatory. A timezone offset is optional; if omitted then UTC is assumed.

The JSON object may optionally contain:

- ``event_type``: A string that specifies the type of event.
- ``parameters``: A JSON value that can contain additional information pertaining to the event.

The server will respond to with a ``204 No Content`` response once it has received the request and will process it further.

.. note::
  The JSON is not parsed and validated before the HTTP response is generated. Invalid JSON or requests that do not contain the mandatory properties will be silently discarded.

Unlike with browser sources, clients are responsible for:

- Generating and maintaining party and session identifiers. These must be globally unique and conform to a specific format, described below. The duration for which a party or session identifier remains valid is determined by the client.
- Generating a unique identifier for each event. This must also be globally unique.

Party and session identifiers must conform to the following format:

.. productionlist:: divolte_identifier
  identifier: "0", ":", `timestamp`, ":", `unique_id`;
  timestamp: [ "-" | "+" ], `digits`+;
  unique_id: { `upper` | `lower` | `digits` | `punct` }+;
  digits: "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
  upper: "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I" | "J" | "K" | "L" | "M" |
       : "N" | "O" | "P" | "Q" | "R" | "S" | "T" | "U" | "V" | "W" | "X" | "Y" | "Z";
  lower: "a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i" | "j" | "k" | "l" | "m" |
       : "n" | "o" | "p" | "q" | "r" | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z";
  punct: "~" | "_" | "!";

A :token:`timestamp` is the time at which the identifier was generated, represented as the `Base36-encoded <https://en.wikipedia.org/wiki/Base36>`_ number of milliseconds since midnight, January 1, 1970 UTC.

Assuming a JSON source has been configured with a path of ``/json-source``, the first two events for a new user could be sent using something like:

.. code-block:: console

  % curl 'https://track.example.com/json-source?p=0:is8tiwk4:GKv5gCc5TtrvBTs9bXfVD8KIQ3oO~sEg' \
      --dump-header - \
      --header 'Content-Type: application/json' \
      --data '
  {
    "session_id": "0:is8tiwk4:XLEUVj9hA6AXRUOp2zuIdUpaeFOC~7AU",
    "event_id": "AruZ~Em0WNlAnbyzVmwM~GR0cMb6Xl9r",
    "is_new_party": true,
    "is_new_session": true,
    "client_timestamp_iso": "2016-08-24T13:29:39.412+02:00",
    "event_type": "newUser",
    "parameters": {
      "channel": "google"
    }
  }'
  HTTP/1.1 204 No Content
  Server: divolte
  Date: Wed, 24 Aug 2016 11:29:39 GMT
  % curl 'https://track.example.com/json-source?p=0:is8tiwk4:GKv5gCc5TtrvBTs9bXfVD8KIQ3oO~sEg' \
      --dump-header - \
      --header 'Content-Type: application/json' \
      --data '
  {
    "session_id": "0:is8tiwk4:XLEUVj9hA6AXRUOp2zuIdUpaeFOC~7AU",
    "event_id": "QSQMAp66OeNX_PooUvdmjNSSn7ffqjAk",
    "is_new_party": false,
    "is_new_session": false,
    "client_timestamp_iso": "2016-08-24T13:29:39.412+02:00",
    "event_type": "screenView",
    "parameters": {
      "screen": "home"
    }
  }'
  HTTP/1.1 204 No Content
  Server: divolte
  Date: Wed, 24 Aug 2016 11:29:39 GMT

Within the namespace for a JSON source properties are used to configure it.

JSON source property: ``event_path``
""""""""""""""""""""""""""""""""""""
:Description:
  The path which should be used for sending events to this source.
:Default:
  ``/``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = json
      event_path = /mob-event
    }

JSON source property: ``party_id_parameter``
""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The name of the query parameter that will contain the party identifier for a request.
:Default:
  ``p``
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = json
      party_id_parameter = id
    }

JSON source property: ``maximum_body_size``
"""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum acceptable size (in bytes) of the JSON body for an event. The HTTP request is aborted as quickly as possible once it becomes apparent this value is exceeded. Clients can use the HTTP Expect/Continue mechanism to determine whether a request body is too large.

:Default:
  4 KB
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = json
      maximum_body_size = 16K
    }

Mappings (``divolte.mappings``)
-------------------------------

Mappings are used to specify event flows between sources and sinks, along with the transformation ("mapping") required to convert events into Avro records that conform to a schema. Schema mapping is an important feature of Divolte Collector as it allows incoming events to be mapped onto custom Avro schemas in non-trivial ways. See :doc:`mapping_reference` for details about this process and the internal mapping DSL used for defining mappings.

Each configured mapping has a name and produces homogenous records conforming to an Avro schema. It may consume events from multiple sources, and the resulting records may be sent to multiple sinks. Sources and sinks may be shared between multiple mappings. If multiple mappings produce records for the same sink, all mappings must use the same Avro schema.

An example mapping configuration could be:

.. code-block:: none

  divolte.mappings {
    // The name of the mapping is 'a_mapping'
    a_mapping = {
      schema_file = /some/dir/MySchema.avsc
      mapping_script_file = schema-mapping.groovy
      sources = [browser]
      sinks = [hdfs,kafka]
    }
  }

Implicit default mapping
^^^^^^^^^^^^^^^^^^^^^^^^

If no mappings are specified a single implicit mapping is created that is equivalent to:

.. code-block:: none

  divolte.mappings {
    // The name of the implicit mapping is 'default'
    default = {
      sources = [ /* All configured sources */ ]
      sinks = [ /* All configured sinks */ ]
    }
  }

If *any* mappings are configured this implicit mapping is not present and all mappings must be explicitly specified.

Mapping properties
^^^^^^^^^^^^^^^^^^

Within the namespace for a mapping properties are used to configure it. At a minimum the ``sources`` and ``sinks`` should be specified; without these a mapping has no work to do.

Mapping property: ``sources``
"""""""""""""""""""""""""""""
:Description:
  A list of the names of the sources that this mapping should consume events from. A source may be shared by multiple mappings; each mapping will process every event from the source.
:Default:
  *Not specified*
:Example:

  .. code-block:: none

    divolte.mappings.a_mapping {
      sources = [site1, site2]
    }

Mapping property: ``sinks``
"""""""""""""""""""""""""""
:Description:
  A list of the names of the sinks that this mapping should write produced Avro records to. Each produced record is written to all sinks. A sink may be shared by multiple mappings; in this case all mappings must produce records conforming to the same Avro schema.
:Default:
  *Not specified*
:Example:

  .. code-block:: none

    divolte.mappings.a_mapping {
      sinks = [hdfs, kafka]
    }

Mapping property: ``schema_file``
"""""""""""""""""""""""""""""""""
:Description:
  By default a mapping will produce records that conform to a `built-in Avro schema <https://github.com/divolte/divolte-schema>`_. However, a custom schema makes usually makes sense that contains fields specific to the domain and custom events. Note that the value for this property is ignored unless ``mapping_script_file`` is also set.
:Default:
  |Built-in schema|_
:Example:

  .. code-block:: none

    divolte.mappings.a_mapping {
      schema_file = /etc/divolte/MyEventRecord.avsc
    }

.. |Built-in schema| replace:: *Built-in schema*
.. _Built-in schema: https://github.com/divolte/divolte-schema

Mapping property: ``mapping_script_file``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  The location of the Groovy script that defines the how events from sources will be mapped to Avro records that are written to sinks. If unset, a default built-in mapping will be used. (In this case any value for the ``schema_file`` property is ignored: the default built-in mapping always produces records conforming to the `built-in schema <https://github.com/divolte/divolte-schema>`.)

  See the :doc:`mapping_reference` for details on mapping events.
:Default:
  *Built-in mapping*
:Example:

  .. code-block:: none

    divolte.mappings.a_mapping {
      mapping_script_file = /etc/divolte/my-mapping.groovy
    }

Mapping property: ``discard_corrupted``
"""""""""""""""""""""""""""""""""""""""
:Description:
  Events contain a flag indicating whether the source detected corruption in the event data. If this property is enabled corrupt events will be discarded and not subject to mapping and further processing. Otherwise a best effort will be made to map and process the event as if it was normal.

  Only browser sources currently detect corruption and set this flag accordingly.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.mappings.a_mapping {
      discard_corrupted = true
    }

Mapping property: ``discard_duplicates``
""""""""""""""""""""""""""""""""""""""""
:Description:
  Clients sometimes deliver events to sources multiple times, normally within a short period of time. Sources contain a probabilistic filter which can detect this and set a flag on the event. If this property is enabled events flagged as duplicates will be discarded without further mapping or processing.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      discard_duplicates = true
    }

Sinks (``divolte.sinks``)
-------------------------

Sinks are used to write Avro records that have been mapped from received events. Each sink has a name used to identify it when configuring a mapper that produces records for the sink. A sink cannot have the same name as a source (and vice versa). Sinks are configured in sections using their name as the configuration path. (Due to the `HOCON merging rules <https://github.com/typesafehub/config/blob/master/HOCON.md#duplicate-keys-and-object-merging>`_, it's not possible to configure multiple sinks with the same name.)

Each sink has a type configured via a mandatory ``type`` property. The supported types are:

- ``hdfs``
- ``kafka``

For example:

.. code-block:: none

  divolte.sinks {
    // The name of the source is 'my_sink'
    my_sink = {
      // This is a HDFS sink.
      type = hdfs
    }
  }

Implicit default sinks
^^^^^^^^^^^^^^^^^^^^^^

If no sinks are specified two implicit sinks are created that are equivalent to:

.. code-block:: none

  divolte.sinks {
    // The name of the implicit sinks are 'hdfs' and 'kakfa'.
    hdfs = {
      type = hdfs
      replication_factor = 1
    }
    kafka = {
      type = kafka
    }
  }

If *any* sinks are configured these implicit sinks are not present and all sinks must be explicitly specified.


HDFS Sinks
^^^^^^^^^^

A HDFS sink uses a HDFS client to write `Avro files <http://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files>`_ containing records produced by mapping. The schema of the Avro file is the schema of the mapping producing the records. If multiple mappings produce records for a sink they must all use the same schema.

The HDFS client used to write files is configured according to the global HDFS settings. Depending on the HDFS client version in use, HDFS sinks can write to various locations:

- Native HDFS in a Hadoop cluster.
- A local filesystem.
- S3 in Amazon Web Services (AWS). (See `here <https://wiki.apache.org/hadoop/AmazonS3>`_ for details.)

A HDFS sink uses multiple threads to write the records as they are produced. Each thread writes to its own Avro file, flushing regularly. Periodically the Avro files are closed and new ones started. Files are initially created in the configured working directory and have an extension of ``.avro.partial`` while open and being written to. When closed, they are renamed to have an extension of ``.avro`` and moved to the publish directory. This happens in a single (atomic) move operation, so long as the underlying storage supports this.

Records produced from events with the same party identifier are always written to the same Avro file, and in the order they were received by the originating source. (The relative ordering of records produced from events with the same party identifier is undefined if they originated from different sources, although they will still be written to the same Avro file.)

Within the namespace for a HDFS sink properties are used to configure it.

HDFS Sink Property: ``replication``
"""""""""""""""""""""""""""""""""""
:Description:
  The HDFS replication factor to use when creating files.
:Default:
  3
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      replication = 1
    }

HDFS Sink Property: ``file_strategy.working_dir``
"""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Directory where files are created and kept while being written to. Files being written have a ``.avro.partial`` extension.

  This directory has to exist when Divolte Collector starts; it will not be automatically created. The user that Divolte Collector is running as needs to have write permissions for this directory.
:Default:
  :file:`/tmp`
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.working_dir = /webdata/inflight
    }

HDFS Sink Property: ``file_strategy.publish_dir``
"""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Directory where files are moved to after they are closed. Files when closed have a ``.avro`` extension.

  This directory has to exist when Divolte Collector starts; it will not be automatically created. The user that Divolte Collector is running as needs to have write permissions for this directory.
:Default:
  :file:`/tmp`
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.publish_dir = /webdata/published
    }

HDFS Sink Property: ``file_strategy.roll_every``
""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Roll over files on HDFS after this amount of time. (If the working file doesn't contain any records it will be discarded.)
:Default:
  1 hour
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.roll_every = 15 minutes
    }

HDFS Sink Property: ``file_strategy.sync_file_after_records``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of records that should be written to the working file since the last flush before flushing again. Flushing is performed by issuing a :code:`hsync()` call to flush HDFS data.
:Default:
  1000
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.sync_file_after_records = 100
    }

HDFS Sink Property: ``file_strategy.sync_file_after_duration``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum time that may elapse after a record is written to the working file before it is flushed. Flushing is performed by issuing a :code:`hsync()` call to flush HDFS data.
:Default:
  30 seconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.sync_file_after_duration = 10 seconds
    }

Kafka Sinks
^^^^^^^^^^^

A Kafka sink uses a Kafka producer to write Avro records as individual messages on a Kafka topic. The producer is configured according to the global Kafka settings.

Records produced from events with the same party identifier are queued on a topic in the same order they were received by the originating source. (The relative ordering across sources is not guaranteed.) The messages are keyed by their party identifier meaning that Kafka will preserve the relative ordering between messages with the same party identifier.

The body of each Kafka message contains a single Avro record, serialised using Avro's `binary encoding <http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding>`_. The schema is not included or referenced in the message. Because Avro's binary encoding is not self-describing, a topic consumer must be independently configured to use a *write schema* that corresponds to the schema used by the mapper that produced the record.

Within the namespace for a Kafka sink properties are used to configure it.

Kafka sink property: ``topic``
""""""""""""""""""""""""""""""
:Description:
  The Kafka topic onto which events are published.
:Default:
  ``divolte``
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = kafka
      topic = clickevents
    }
