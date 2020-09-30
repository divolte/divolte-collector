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

.. envvar:: GOOGLE_APPLICATION_CREDENTIALS

:Description:
  If using sinks that write to Google Cloud Storage or Pub/Sub, this environment variable is checked for the location of the file containing credentials for writing to your buckets. For more information about this environment variable please refer to Google's documentation on `How Application Default Credentials work <https://developers.google.com/identity/protocols/application-default-credentials>`_.

:Example:

  .. code-block:: bash

    GOOGLE_APPLICATION_CREDENTIALS='/etc/divolte/gcs-credentials.json'

.. envvar:: GOOGLE_CLOUD_PROJECT

:Description:
  If using sinks that use Google Cloud Pub/Sub, this environment variable is checked for the project id where the topics to use are defined.

:Example:

  .. code-block:: bash

    GOOGLE_CLOUD_PROJECT='click-stream-collection'

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

Property: ``divolte.global.server.debug_requests``
""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When true Divolte Collector logs (with great verbosity) all HTTP requests and responses.
  This is intended purely for development or debugging and should never be enabled on a
  production system.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.global.server {
      debug_requests = true
    }

Property: ``divolte.global.server.shutdown_delay``
""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When a shutdown signal (``TERM``) is received the shutdown will be delayed for this
  long before actually starting. During this interval all HTTP requests will be
  served as usual, with the exception of the ``/ping`` endpoint which will return
  a response with a 503 HTTP status code. The purpose of this is delay is allow
  load balancers to gracefully remove the server from service without disrupting normal
  traffic.
:Default:
  0 milliseconds
:Example:

  .. code-block:: none

    divolte.global.server {
      shutdown_delay = 30 seconds
    }

Property: ``divolte.global.server.shutdown_timeout``
""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  When shutting down, the server will wait for existing HTTP requests to complete. This
  setting controls how long the server will wait before shutting down anyway even if some
  HTTP requests are still underway.
:Default:
  2 minutes
:Example:

  .. code-block:: none

    divolte.global.server {
      shutdown_timeout = 1 minute
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

    divolte.global.hdfs {
      buffer_size = 1048576
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

    divolte.global.kafka {
      buffer_size = 1048576
    }

Property: ``divolte.global.kafka.producer``
"""""""""""""""""""""""""""""""""""""""""""
:Description:
  The configuration to use for Kafka producers. All settings are used as-is to configure the Kafka producer; refer to the `Kafka Documentation <https://kafka.apache.org/documentation.html#producerconfigs>`_ for further details.
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
      bootstrap.servers = ["server1:9092", "server2:9092", "server3:9092"]
      client.id = divolte.collector

      acks = 0
      retries = 5
    }

Global Google Cloud Storage Settings (``divolte.global.gcs``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls global Google Cloud Storage settings shared by all Google Cloud Storage sinks.

Property: ``divolte.global.gcs.enabled``
""""""""""""""""""""""""""""""""""""""""
:Description:
  Whether or not Google Cloud Storage support is enabled. When set to `false` all Google Cloud Storage sinks are ignored.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.global.gcs {
      enabled = true
    }

Property: ``divolte.global.gcs.threads``
""""""""""""""""""""""""""""""""""""""""
:Description:
  Number of threads to use per Google Cloud Storage sink for writing events. Each thread creates its own files on Google Cloud Storage.
:Default:
  1
:Example:

  .. code-block:: none

    divolte.global.gcs {
      threads = 2
    }

Property: ``divolte.global.gcs.buffer_size``
""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of mapped events to queue internally *per sink thread* for Google Cloud Storage before starting to drop them. This value will be rounded up to the nearest power of 2.
:Default:
  1048576
:Example:

  .. code-block:: none

    divolte.global.gcs {
      buffer_size = 1048576
    }

Global Google Cloud Pub/Sub Settings (``divolte.global.gcps``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section controls global Google Cloud Pub/Sub settings shared by all Google Cloud Pub/Sub sinks.

Property: ``divolte.global.gcps.enabled``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  Whether or not Google Cloud Pub/Sub support is enabled. When set to `false` all Google Cloud Pub/Sub sinks are ignored.
:Default:
  :code:`false`
:Example:

  .. code-block:: none

    divolte.global.gcps {
      enabled = true
    }

Property: ``divolte.global.gcps.threads``
"""""""""""""""""""""""""""""""""""""""""
:Description:
  Number of threads to use per Google Cloud Pub/Sub sink for writing events. Each thread creates its own files on Google Cloud Pub/Sub.
:Default:
  2
:Example:

  .. code-block:: none

    divolte.global.gcps {
      threads = 1
    }

Property: ``divolte.global.gcps.buffer_size``
"""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of mapped events to queue internally *per sink thread* for Google Cloud Pub/Sub before starting to drop them. This value will be rounded up to the nearest power of 2.
:Default:
  1048576
:Example:

  .. code-block:: none

    divolte.global.gcps {
      buffer_size = 1048576
    }

Property: ``divolte.global.gcps.project_id``
""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The `project id`_ in which the the topics are located where Divolte will publish events. If Google Cloud Pub/Sub is enabled, then either this property must be set *or* a default project id must be available via the context in which the application is running.
:Default:
  *Not specified*
:Example:

  .. code-block:: none

    divolte.global.gcps {
      project_id = divolte-event-ingest
    }

.. _project id: https://support.google.com/cloud/answer/6158840?hl=en

  It is also possible to configure divolte to work with a kerberized Kafka cluster the following configuration snippet shows how.


  .. code-block:: none

    divolte.global.kafka.producer = {
      bootstrap.servers = ["server1:9092", "server2:9092", "server3:9092"]
      client.id = divolte.collector

      acks = 0
      retries = 5

      sasl.jaas.config = ""
      sasl.jaas.config = ${?KAFKA_SASL_JAAS_CONFIG}

      security.protocol = PLAINTEXT
      security.protocol = ${?KAFKA_SECURITY_PROTOCOL}
      sasl.mechanism = GSSAPI
      sasl.kerberos.service.name = kafka
    }

  The :envvar:`KAFKA_SECURITY_PROTOCOL` can then be set to `SASL_PLAINTEXT` and the :envvar:`KAFKA_SASL_JAAS_CONFIG` can be set to something like:

  .. code-block:: none

    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/divolte.keytab"
    principal="divolte/hostname.divolte.io";


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

Signalled events are delivered to the browser source by the page in the background in the order they were signalled. Any pending events not yet delivered may be discarded if the page is closed or the user navigates to a new page. The tag provides an API for the page to detect when it is safe to leave the page:

.. code-block:: html

  <script>
    divolte.whenCommitted(function() {
      // Invoked when previously signalled events are no longer in danger of being discarded.
    }, 1000);
  </script>

The first argument is a callback that will be invoked when it is safe to leave the page without losing previously signalled events. The second argument is an optional timeout (specified in milliseconds) after which the callback will be invoked even if previously signalled events may be dropped. A timeout can occur, for example, if it is taking too long to deliver events to the browser source.

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

Browser source property: ``http_response_delay``
""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  This property can be used to introduce an artificial delay when an event is received
  before sending the HTTP response. This is intended only for for testing purposes, and
  should never be changed from the default in production. (Note that only the HTTP
  response is delayed; the event is processed internally without delay.)
:Default:
  0 seconds
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      http_response_delay = 2 seconds
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

Browser source property: ``javascript.event_timeout``
"""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The JavaScript tag delivers events in the order they are generated, waiting for the previous event to be delivered before
  sending the next. This property specifies a timeout after which the tag proceeds with the next event even if the previous
  has not been delivered yet.
:Default:
  750 milliseconds
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = browser
      javascript.event_timeout = 1 second
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
  4096
:Example:

  .. code-block:: none

    divolte.sources.a_source {
      type = json
      maximum_body_size = 16384
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

Mapping property: ``confluent_id``
""""""""""""""""""""""""""""""""""
:Description:
  The identifier of the Avro schema, if it has been registered with with the Confluent Schema Registry. This is required if any Kafka sinks are associated with this mapping and are configured to operate in ``confluent`` mode.
:Default:
    *Not specified*
:Example:

    .. code-block:: none

      divolte.mappings.a_mapping {
        confluent_id = 1234
      }

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

- File based sinks:

  - ``hdfs``
  - ``gcs``

- Streaming sinks:

  - ``kafka``
  - ``gcps``

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

File Based Sinks
^^^^^^^^^^^^^^^^

A file based sink writes `Avro files <http://avro.apache.org/docs/1.9.0/spec.html#Object+Container+Files>`_ containing records produced by mapping to a remote file system. The schema of the Avro file is the schema of the mapping producing the records. If multiple mappings produce records for a sink they must all use the same schema.

File based sinks use multiple threads to write the records as they are produced. Each thread writes to its own Avro file, flushing regularly. Periodically the Avro files are closed and new ones started. Files are initially created in the configured working directory and have an extension of ``.avro.partial`` while open and being written to. When closed, they are renamed to have an extension of ``.avro`` and moved to the publish directory. This happens in a single (atomic) move operation, so long as the underlying storage supports this.

Records produced from events with the same party identifier are always written to the same Avro file, and in the order they were received by the originating source. (The relative ordering of records produced from events with the same party identifier is undefined if they originated from different sources, although they will still be written to the same Avro file.)

The supported types of file based sinks are:

- HDFS
- Google Cloud Storage (Experimental)

The following properties are common to all file based sinks:

File Based Sink Property: ``file_strategy.working_dir``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Directory where files are created and kept while being written to. Files being written have a ``.avro.partial`` extension.

:Default:
  :file:`/tmp`
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.working_dir = /webdata/inflight
    }

File Based Sink Property: ``file_strategy.publish_dir``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Directory where files are moved to after they are closed. Files when closed have a ``.avro`` extension.

:Default:
  :file:`/tmp`
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.publish_dir = /webdata/published
    }

File Based Sink Property: ``file_strategy.roll_every``
""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  Roll over files on the remote file system after this amount of time. If the working file doesn't contain any records it will be discarded.
:Default:
  1 hour
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.roll_every = 15 minutes
    }

File Based Sink Property: ``file_strategy.sync_file_after_records``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of records that should be written to the working file since the last flush before flushing to the remote file system again. If the remote file system permits, flushing will also attempt to fsync the file to the file system (e.g. by issuing a :code:`hsync()` call in case of HDFS data).
:Default:
  1000
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.sync_file_after_records = 100
    }

File Based Sink Property: ``file_strategy.sync_file_after_duration``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum time that may elapse after a record is written to the working file before it is flushed to the remote file system. If the remote file system permits, flushing will also attempt to fsync the file to the file system (e.g. by issuing a :code:`hsync()` call in case of HDFS data).
:Default:
  30 seconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = hdfs
      file_strategy.sync_file_after_duration = 10 seconds
    }


HDFS Sinks
^^^^^^^^^^

The HDFS client used to write files is configured according to the global HDFS settings. Depending on the HDFS client version in use, HDFS sinks can write to various locations:

- Native HDFS in a Hadoop cluster.
- A local filesystem.
- S3 in Amazon Web Services (AWS). (See `here <https://wiki.apache.org/hadoop/AmazonS3>`_ for details.)

*When writing to HDFS, the configured directories for inflight and published files have to exist when Divolte Collector starts; they will not be automatically created. The available privileges to Divolte Collector need to allow for writing in this directory.*

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

Google Cloud Storage Sinks
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

  Support for Google Cloud Storage is currently experimental.

A built in HTTP client is used to write files to Google Cloud Storage.

*When writing to Google Cloud Storage, the configured bucket must exist when Divolte Collector starts; no attempt is made to create a bucket. The available privileges to Divolte Collector need to allow for writing in this bucket.*

For authentication against Google Cloud services, Divolte Collector expects `Application Default Credentials <https://developers.google.com/identity/protocols/application-default-credentials>`_ to be configured on the host running Divolte Collector. No other mechanism for authenticating against Google is currently supported. When running on Google Cloud infrastructure, application default credentials are usually setup correctly. To setup application default credentials in other environments, consider the `relevant documentation from Google <https://developers.google.com/identity/protocols/application-default-credentials#howtheywork>`_.

Divolte Collector has configurable settings for when to flush/sync data to a file and when to roll the file and start a new one. On many filesystems, there is direct lower level support for these operations (e.g. HDFS). On Google Cloud Storage, these concepts don't map directly onto operations supported by the storage mechanism. As a result, Divolte Collector translates file creation, syncing and rolling into the following operations on Google Cloud Storage:

+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Action        | GCS operations                                                                                                                                                                                        |
+===============+=======================================================================================================================================================================================================+
| Open new file | Create a new file on GCS that only contains the Avro file header (but no data) using the `simple upload <https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload>`_ mechanism on GCS. |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Sync file     | 1. Create a new file on GCS that contains only the encoded flushed records using the `simple upload <https://cloud.google.com/storage/docs/json_api/v1/how-tos/simple-upload>`_ mechanism on GCS.     |
|               | 2. Merge the existing file with the new one written as part of this sync operation into one file using GCS' `compose API <https://cloud.google.com/storage/docs/json_api/v1/objects/compose>`_.       |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Roll file     | 1. Write any remaining buffered data as above during a sync operation.                                                                                                                                |
|               | 2. Use the `compose API <https://cloud.google.com/storage/docs/json_api/v1/objects/compose>`_ to merge the existing file with the final part, but set the destination to the publish dir.             |
|               | 3. Delete the remaining file in the working dir.                                                                                                                                                      |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

To enable this behaviour, Divolte Collector performs in-memory buffering up to a number of records before sending the data to Google Cloud Storage. The maximum number of records that the sink will buffer is equal to the sink's configuration setting ``file_strategy.sync_file_after_records``. When the buffer fills up or earlier when the time configured in the sink's ``file_strategy.sync_file_after_duration`` expires, Divolte Collector will write a partial file

*Given the behaviour described above, it is advised to set the sync duration and maximum number of un-synced records to larger than default values when writing to Google Cloud Storage in production settings. When using defaults in a high traffic environment, you are likely to make too many API calls to Google and hit rate limits.* When configuring Divolte Collector for Google Cloud Storage, pay attention to the `Best Practices for Google Cloud Storage <https://cloud.google.com/storage/docs/best-practices>`_ with your expected traffic volume in mind.

A Google Cloud Storage sink can use multiple threads to write the records as they are produced. Each thread writes to its own Avro files. Records produced from events with the same party identifier are always written to the same Avro file, and in the order they were received by the originating source. (The relative ordering of records produced from events with the same party identifier is undefined if they originated from different sources, although they will still be written to the same Avro file.)

The minimum required configuration for a Google Cloud Storage sink is the name of the bucket to write to. In addition to this a group of retry settings can be specified; these control the internal retry behaviour when failures occur interacting with the Google Cloud Storage APIs.

Google Cloud Storage Sink Property: ``bucket``
""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The Google Cloud Storage bucket name to write the files to. The configured directories in the file strategy for this sink will be relative to the root of this bucket.
:Default:
  *Not specified*
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
    }

Google Cloud Storage Sink Property: ``retry_settings.max_attempts``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of times the sink will attempt a specific API call before giving up. A value of ``0`` means there is no such limit.
:Default:
    ``0``
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.max_attempts = 10
    }

Google Cloud Storage Sink Property: ``retry_settings.total_timeout``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The total duration for which the sink will attempt a specific API call before giving up.
:Default:
    10 minutes
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.total_timeout = 30 minutes
    }

Google Cloud Storage Sink Property: ``retry_settings.initial_retry_delay``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How long the sink will wait before the first time it retries an API call that has failed.
:Default:
    1 second
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.initial_retry_delay = 10 milliseconds
    }

Google Cloud Storage Sink Property: ``retry_settings.retry_delay_multiplier``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How much the wait used by the sink between retries of a failed API call should be multiplied by before each subsequent retry.
:Default:
    2
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.retry_delay_multiplier = 1.5
    }

Google Cloud Storage Sink Property: ``retry_settings.max_retry_delay``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum delay used by the sink between retries when an API call has failed.
:Default:
    64 seconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.max_retry_delay = 30 seconds
    }

Google Cloud Storage Sink Property: ``retry_settings.jitter_delay``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  If set, a random delay up to this amount will be added to or subtracted from each delay between retry attempts after an API call has failed. Note that either this setting or ``jitter_factor`` may be specified, but not both.
:Default:
    1 second (if ``jitter_factor`` has not been specified)
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.jitter_delay = 500 ms
    }

Google Cloud Storage Sink Property: ``retry_settings.jitter_factor``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  If set, each delay between retry attempts after an API call has failed will be scaled by a random amount up to this proportion of the nominal delay for a particular attempt. For example, if set to 0.1 and a retry was due to take place 20 seconds after the previous attempt, the actual delay would be a between 18 and 22 seconds. Note that either this setting or ``jitter_delay`` may be specified, but not both.
  The maximum delay used by the sink between retries when an API call has failed.
:Default:
    *Not set*
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcs
      bucket = my_organisation_web_data
      retry_settings.jitter_factor = 0.25
    }

Topic Based Sinks
^^^^^^^^^^^^^^^^^

A topic based sink publishes events as Avro records to a topic for consumption in real-time. Each event is published as a single message on the topic.

The supported types of topic based sinks are:

- Kafka
- Google Cloud Pub/Sub (Experimental)

The following properties are common to all topic based sinks:

Topic Based Sink Property: ``topic``
""""""""""""""""""""""""""""""""""""
:Description:
  The name of the topic onto which events will be published.
:Default:
  ``divolte``
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = kafka
      topic = clickevents
    }

Kafka Sinks
^^^^^^^^^^^

A Kafka sink publishes each event to a Kafka topic. The Kafka producer used to do this is configured according to the global Kafka settings.

Records produced from events with the same party identifier are queued on a topic in the same order they were received by the originating source. (The relative ordering across sources is not guaranteed.) The messages are keyed by their party identifier meaning that Kafka will preserve the relative ordering between messages with the same party identifier.

The body of each Kafka message contains a single Avro record, serialized in one of two possible ways depending on the sink mode:

- In ``naked`` mode (the default) the record is serialised using Avro's `binary encoding <http://avro.apache.org/docs/1.9.0/spec.html#binary_encoding>`_. The schema is not included or referenced in the message. Because Avro's binary encoding is not self-describing, a topic consumer must be independently configured to use a *write schema* that corresponds to the schema used by the mapper that produced the record.
- In ``confluent`` mode (experimental) the record is serialized using the `wire format for the Confluent platform <https://docs.confluent.io/3.3.0/schema-registry/docs/serializer-formatter.html#wire-format>`_. This requires that mappings for this sink be configured with the ``confluent_id`` specifying the identifier of the Avro schema as registered in the Schema Registry. (Divolte does not register the schema itself.)

Within the namespace for a Kafka sink properties are used to configure it.

Kafka Sink Property: ``mode``
"""""""""""""""""""""""""""""
:Description:
  The Kafka sink mode, which controls how Avro records are formatted as Kafka messages:

  - In ``naked`` mode the records are serialised using Avro's `binary encoding <http://avro.apache.org/docs/1.9.0/spec.html#binary_encoding>`_.
  - In ``confluent`` mode (experimental) the records are serialised using `Confluent platform's wire format <https://docs.confluent.io/3.3.0/schema-registry/docs/serializer-formatter.html#wire-format>`_. This only affects the message body.

  Note that ``confluent`` mode is only permitted if the ``confluent_id`` is specified (and the same) for all mappings that this sink consumes from.
:Default:
  ``naked``
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = kafka
      mode = confluent
    }

.. _pubsub-sinks-label:

Google Cloud Pub/Sub Sinks
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

  Support for Google Cloud Storage is currently experimental.

A Google Cloud Pub/Sub sink sends Avro records as messages to a Pub/Sub topic. The topic must exist when Divolte Collector starts; no attempt is made to create it on startup, and privileges must be available to allow for writing to it.

For authentication against Google Cloud services, Divolte Collector expects `Application Default Credentials <https://developers.google.com/identity/protocols/application-default-credentials>`_ to be configured on the host running Divolte Collector. No other mechanism for authenticating against Google is currently supported. When running on Google Cloud infrastructure, application default credentials are usually available implicitly. To setup application default credentials in other environments, consider the `relevant documentation from Google <https://developers.google.com/identity/protocols/application-default-credentials#howtheywork>`_.

Messages produced from events with the same party identifier are sent to the topic in the same order they were received by the originating source. (The relative ordering across sources is not guaranteed.) Pub/Sub itself offers best-effort ordering for receipt by subscribers but this is not guaranteed.

The data in each Pub/Sub message contains a single Avro record, serialised using Avro's `binary encoding <http://avro.apache.org/docs/1.9.0/spec.html#binary_encoding>`_. In addition to the data, each message has several attributes set:

:partyIdentifier:
  The party id associated with this event.
:schemaFingerprint:
  This is set to the `schema fingerprint <https://avro.apache.org/docs/1.9.0/spec.html#schema_fingerprints>`_ of the Avro schema used to encode the message, calculated using the `SHA-256 digest algorithm <http://en.wikipedia.org/wiki/SHA-2>`_. The digest itself is encoded using the ``base64url`` encoding `specified in RFC4648 <https://tools.ietf.org/html/rfc4648#section-5>`_, without padding.
:schemaConfluentId:
  This attribute is only set if the mapping used to produce the record specifies a ``confluent_id``. In this case the attribute contains that value, encoded using base 16.

Within the namespace for a Pub/Sub sink properties are used to configure it. These are grouped into into:

- *Retry settings*: these control the internal retry behaviour of the underlying SDK when failures occur. Note that in general Divolte will attempt redelivery indefinitely if the underlying SDK indicates a retry might succeed. When this is not the case a message is abandoned.
- *Batching settings*: these control the way the underlying SDK will accumulate messages for publication as a batch to improve performance.

Google Cloud Pub/Sub Sink Property: ``retry_settings.max_attempts``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The number of times the internal publisher will attempt delivery before giving up. A value of ``0`` means there is no such limit.
:Default:
  ``0``
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.max_attempts = 10
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.total_timeout``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The total duration for which the internal publisher will attempt delivery before giving up.
:Default:
  10 seconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.total_timeout = 1 minute
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.initial_retry_delay``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How long the internal publisher will wait if the first delivery attempt fails before the first retry.
:Default:
  5 milliseconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.initial_retry_delay = 10 milliseconds
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.retry_delay_multiplier``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How much the wait used by the internal publisher between retries should be multiplied by before each subsequent retry.
:Default:
  2
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.retry_delay_multiplier = 1.5
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.max_retry_delay``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum delay used by the internal publisher between retries when delivery of a message fails.
:Default:
  1 minute
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.max_retry_delay = 30 seconds
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.initial_rpc_timeout``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How long the internal publisher will wait for the first RPC to succeed.
:Default:
  15 seconds
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.initial_rpc_timeout = 10 seconds
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.rpc_timeout_multiplier``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  How much the RPC timeout used by the internal publisher between retries should be multiplied by before each subsequent retry.
:Default:
  2
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.rpc_timeout_multiplier = 1.5
    }

Google Cloud Pub/Sub Sink Property: ``retry_settings.max_rpc_timeout``
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum RPC timeout used by the internal publisher when retrying.
:Default:
  *The value of ``initial_rpc_timeout``.*
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      retry_settings.max_rpc_timeout = 10 seconds
    }

Google Cloud Pub/Sub Sink Property: ``batching_settings.element_count_threshold``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum number of messages that should be accumulated before triggering delivering the batch.
:Default:
  100
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      batching_settings.element_count_threshold = 1000
    }

Google Cloud Pub/Sub Sink Property: ``batching_settings.request_bytes_threshold``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum total size of the accumulated messages that should be allowed before triggering delivery of the batch.
:Default:
  1000
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      batching_settings.request_bytes_threshold = 65536
    }

Google Cloud Pub/Sub Sink Property: ``batching_settings.delay_threshold``
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
:Description:
  The maximum time to wait for additional messages to accumulate before triggering delivery of the batch.
:Default:
  1 millisecond
:Example:

  .. code-block:: none

    divolte.sinks.a_sink {
      type = gcps
      batching_settings.delay_threshold = 500 ms
    }
