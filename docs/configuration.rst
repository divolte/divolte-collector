*************
Configuration
*************
This chapter describes the configuration mechanisms and available options for Divolte Collector. If you are completely new to Divolte Collector, it is probably a good idea to go through the :doc:`getting_started` guide first.

Configuration files
===================
The configuration for Divolte Collector consists of three files:

- ``divolte-env.sh``: a shell script that is included in the startup script to set environment variables and JVM startup arguments.
- ``divolte-collector.conf``: the main configuration file for Divolte Collector.
- ``logback.xml``: the logging configuration.

Configuration directory
-----------------------
Divolte Collector will try to find configuration files at startup in the configuration directory. Typically this is the ``conf/`` directory nested under the Divolte Collector installation. Divolte Collector will try to locate the configuration directory at ``../conf`` relative to the startup script. The configuration directory can be overridden by setting the ``DIVOLTE_CONF_DIR`` environment variable. If set, the value will be used as configuration directory. If you have installed Divolte Collector from a RPM, the init script will set this variable to ``/etc/divolte-collector``.

divolte-env.sh
--------------
This shell script is run by the startup script prior to starting the Divolte Collector JVM. The following environment variables influence Divolte Collector.

HADOOP_CONF_DIR
^^^^^^^^^^^^^^^
:Description:
  Directory where Hadoop / HDFS configuration files are to be found. This directory is included in the classpath on startup, which causes the HDFS client to load the configuration files.

:Example:

  .. code-block:: bash

    HADOOP_CONF_DIR=/etc/hadoop/conf

JAVA_HOME
^^^^^^^^^
:Description:
  The directory where the JRE/JDK is located. Divolte Collector will use ``$JAVA_HOME/bin/java`` as Java executable for startup. If this is not set, Divolte Collector will attempt to find a suitable JDK in a number of common Java installation locations on Linux systems. It is however not recommended to rely on this mechanism for production use.

:Example:

  .. code-block:: bash

    JAVA_HOME=/usr/java/default

DIVOLTE_JAVA_OPTS
^^^^^^^^^^^^^^^^^
:Description:
  Additional arguments passed to the Java Virtual Machine on startup. If not set, by default Divolte Collector will start the JVM with ``-XX:+UseG1GC -Djava.awt.headless=true``. It is recommended to use the G1 garbage collector. For light and medium traffic, the defaults tend to work fine. *If this setting is set, Divolte Collector will not add any arguments by itself; this setting overrides the defaults.*

:Example:

  .. code-block:: bash

    DIVOLTE_JAVA_OPTS="-XX:+UseG1GC -Djava.awt.headless=true -XX:+HeapDumpOnOutOfMemoryError"

divolte-collector.conf
----------------------
This is the main configuration file for Divolte Collector. For configuration, Divolte Collector uses the `Typesafe Config library <https://github.com/typesafehub/config>`_. The dialect of the configuration file is a JSON superset called HOCON (for *Human-Optimized Config Object Notation*). HOCON has a nested structure, like JSON, but is slightly less verbose and doesn't require escaping and quoting of strings in many cases. Here we outline some basic features of HOCON.

Nesting and dot separated namespacing can be used interchangeably:

.. code-block:: none

  // This:
  divolte {
    server {
      host = 127.0.0.1
    }
  }

  // Is the same as this:
  divolte.server.host = 127.0.0.1

Environment variable overrides can be used. In this example the ``divolte.server.port`` setting defaults to 8290, unless the ``DIVOLTE_PORT`` environment variable is set:

.. code-block:: none

  divolte {
    server {
      port = 8290
      port = ${?DIVOLTE_PORT}
    }
  }

Objects are merged:

.. code-block:: none

  // This configuration
  divolte {
    server {
      host = 0.0.0.0
    }
  }

  divolte.server {
    port = 8290
  }

  // Will result in this:
  divolte.server.host = 0.0.0.0
  divolte.server.port = 8290

For a full overview please refer to the `HOCON features and specification <https://github.com/typesafehub/config/blob/master/HOCON.md>`_.

.. warning::

  Be careful when enclosing values in quotes. Quotes are optional, but if present they must be JSON-style double-quotes (``"``).
  This can easily lead to confusion:

  .. code-block:: none

    // This ...
    divolte.tracking.cookie_domain = '.example.com'
    // ... is really equivalent to:
    divolte.tracking.cookie_domain = "'.example.com'"

Configuration reference
=======================
The following sections and settings are available in the ``divolte-collector.conf`` file. Note that in this documentation the path notation for configuration options is used (e.g. ``divolte.server``) but in examples the path and nested notation is used interchangeably.

divolte.server
--------------
This section controls the settings for the internal HTTP server of Divolte Collector.

divolte.server.host
^^^^^^^^^^^^^^^^^^^
:Description:
  The address to which the server binds. Set to a specific IP address to selectively listen on that interface.
:Default:
  ``0.0.0.0``
:Example:

  .. code-block:: none

    divolte.server {
      host = 0.0.0.0
    }

divolte.server.port
^^^^^^^^^^^^^^^^^^^
:Description:
  The TCP port on which the server listens.
:Default:
  ``8290``
:Example:

  .. code-block:: none

    divolte.server {
      port = 8290
    }

divolte.server.use_x_forwarded_for
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Whether to use the ``X-Forwarded-For`` HTTP header for determining the source IP of a request, if present. If multiple values are present, the last value is used.

  Both of these examples would yield a source IP of ``11.34.82.30``:

  1. | ``X-Forwarded-For: 10.200.13.28, 11.45.82.30``
  2. | ``X-Forwarded-For: 10.200.13.28``
     | ``X-Forwarded-For: 11.45.82.30``

:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.server {
      use_x_forwarded_for = true
    }

divolte.server.serve_static_resources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When true Divolte Collector serves a static test page at ``/``.
:Default:
  ``true``
:Example:

  .. code-block:: none

    divolte.server {
      serve_static_resources = false
    }

divolte.tracking
----------------
This section controls the tracking mechanism for Divolte Collector, covering areas such as the cookies and session timeouts, user agent parsing and ip2geo lookups.

divolte.tracking.party_cookie
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The name of the cookie used for setting a party ID.
:Default:
  ``_dvp``
:Example:

  .. code-block:: none

    divolte.tracking {
      party_cookie = _pid
    }

divolte.tracking.party_timeout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The expiry timeout for the party identifier. If no events occur for this duration, the party identifier is discarded.
  Any subsequent events will be assigned a new party identifier.
:Default:
  730 days
:Example:

  .. code-block:: none

    divolte.tracking {
      party_timeout = 1000 days
    }

divolte.tracking.session_cookie
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The name of the cookie used for tracking the session ID.
:Default:
  ``_dvs``
:Example:

  .. code-block:: none

    divolte.tracking {
      session_cookie = _sid
    }

divolte.tracking.session_timeout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The expiry timeout for a session. A session lapses if no events occur for this duration.
:Default:
  30 minutes
:Example:

  .. code-block:: none

    divolte.tracking {
      session_timeout = 1 hour
    }

divolte.tracking.cookie_domain
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The cookie domain that is assigned to the cookies. When left empty, the cookies will have no domain explicitly associated with them, which effectively sets it to the website domain of the page that loaded the Divolte Collector JavaScript.
:Default:
  *Empty*
:Example:

  .. code-block:: none

    divolte.tracking {
      cookie_domain = ".example.com"
    }

divolte.tracking.ip2geo_database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This configures the ip2geo database for geographic lookups. An ip2geo database can be obtained from `MaxMind <https://www.maxmind.com/en/geoip2-databases>`_. (Free 'lite' versions and commercial versions are available.)

  By default no database is configured. When this setting is absent no attempt will be made to lookup geographic-coordinates for IP addresses. When configured, Divolte Collector monitors the database file and will reload it automatically if it changes.
:Default:
  *Not set*
:Example:

  .. code-block:: none

    divolte.tracking {
      ip2geo_database = "/etc/divolte/ip2geo/GeoLite2-City.mmdb"
    }

divolte.tracking.ua_parser
--------------------------
This section controls the user agent parsing settings. The user agent parsing is based on an `open source parsing library <https://github.com/before/uadetector>`_ and supports dynamic reloading of the backing database if an internet connection is available.

divolte.tracking.ua_parser.type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

    divolte.tracking.ua_parser {
       type = caching_and_updating
    }

divolte.tracking.ua_parser.cache_size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  User agent parsing is a relatively expensive operation that requires many regular expression evaluations. Very often the same user agent will make consecutive requests and many clients will have the exact same user agent as well. It therefore makes sense to cache the parsing results for re-use in subsequent requests. This setting determines how many unique user agent strings will be cached.
:Default:
  1000
:Example:

  .. code-block:: none

    divolte.tracking.ua_parser {
      cache_size = 10000
    }

divolte.tracking.schema_file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  By default, Divolte Collector will use a built-in Avro schema for writing data and a default mapping, which is documented in the Mapping section of the user documentation. If not set, a `default built-in schema <https://github.com/divolte/divolte-schema>`_ will be used.

  Typically, users will configure their own schema, usually with fields specific to their domain and custom events and other mappings. When using a user defined schema, it is also required to provide a mapping script. See :doc:`mapping_reference` for further reference.
:Default:
  *`Built-in schema <https://github.com/divolte/divolte-schema>`_*
:Example:

  .. code-block:: none

    divolte.tracking {
      schema_file = /etc/divolte/MyEventRecord.avsc
    }

divolte.tracking.schema_mapping
-------------------------------
This section controls the schema mapping to use. Schema mapping is an important feature of Divolte Collector, as it allows users to map incoming requests onto custom Avro schemas in non-trivial ways. See :doc:`mapping_reference` for details about this process and the internal mapping DSL used for defining mappings.

divolte.tracking.schema_mapping.version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Prior versions of Divolte Collector supported an alternative mapping DSL. The current version is 2, and this is the only
  value supported if the built-in mapping is not being used.
:Default:
  *Not set (for built-in mapping)*
:Example:

  .. code-block:: none

    divolte.tracking.schema_mapping {
      version = 2
    }

divolte.tracking.schema_mapping.mapping_script_file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The location of the Groovy script that defines the how events will be mapped to Avro records. If unset, a default built-in mapping will be used.
:Default:
  *Built-in mapping*
:Example:

  .. code-block:: none

    divolte.tracking.schema_mapping {
      mapping_script_file = /etc/divolte/my-mapping.groovy
    }

divolte.javascript
------------------
This section controls various aspects of the JavaScript tag that will be loaded.

divolte.javascript.name
^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The path with which the JavaScript is served. This changes the ``divolte.js`` part in the script url: http://example.com/divolte.js.
:Default:
  ``divolte.js``
:Example:

  .. code-block:: none

    divolte.javascript {
      name = tracking.js
    }

divolte.javascript.logging
^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Enable or disable the logging on the JavaScript console in the browser.
:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.javascript {
      logging = true
    }

divolte.javascript.debug
^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When enabled, the served JavaScript will be less compact and *slightly* easier to debug. This setting is mainly intended
  to help track down problems in either the minification process used to reduce the size of the tracking script, or in the
  behaviour of specific browser versions.
:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.javascript {
      debug = true
    }

divolte.javascript.auto_page_view_event
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When enabled the JavaScript tag automatically generates a ``pageView`` event when loaded, simplifying site integration.
  If sites wish to control all events (including the initial ``pageView`` event) this can be disabled.
:Default:
  ``true``
:Example:

  .. code-block:: none

    divolte.javascript {
      auto_page_view_event = false
    }


divolte.incoming_request_processor
----------------------------------
This section controls settings related to the processing of incoming requests after they have been received by the server. Incoming requests for Divolte Collector are responded to as quickly as possible, with mapping and flushing occurring in the
 background. Only minimal validation is performed before issuing a HTTP `200 OK` response that contains a transparent 1x1 pixel GIF image.containing a handled by a pool of HTTP threads, which immediately respond with a HTTP code 200 and send the response payload (a 1x1 pixel transparent GIF image). The background mapping and processing is performed by the incoming request processor and configured in this section.

divolte.incoming_request_processor.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for processing incoming requests. All requests for a single party are processed on the same thread.
:Default:
  ``2``
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      threads = 1
    }

divolte.incoming_request_processor.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum number of incoming requests to queue for processing *per thread* before starting to drop incoming requests. While this queue is full new requests are dropped and a warning is logged. (Dropped requests are not reported to the client: Divolte Collector will always respond with a HTTP 200 status code once minimal validation has taken place.).
:Default:
  ``100000``
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      max_write_queue = 1000000
    }

divolte.incoming_request_processor.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum time to wait if the queue is full before dropping an event.
:Default:
  1 second
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      max_enqueue_delay = 20 seconds
    }

divolte.incoming_request_processor.discard_corrupted
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Events from the JavaScript tag contain a checksum to detect corrupted events. (The most common source of this is
  URLs being truncated, but there are also other sources of corruption between the client and the server.) If enabled,
  corrupt events will be discarded and not subject to mapping and further processing. If disabled, a best effort will
  be made to map and process the event as if it was normal.
:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      discard_corrupted = true
    }

divolte.incoming_request_processor.duplicate_memory_size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Browsers and other clients will sometimes deliver an event to the Divolte Collector multiple times, normally
  within a short period of time. Divolte Collector contains a probabilistic filter which can detect this, trading
  off memory for improved results. This setting configures the size of the filter *per thread*, and is multuplied
  by 8 to yield the actual memory usage.
:Default:
  ``1000000``
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      duplicate_memory_size = 10000000
    }

divolte.incoming_request_processor.discard_duplicates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Browsers and other clients will sometimes deliver an event to the Divolte Collector multiple times, normally
  within a short period of time. Divolte Collector contains a probabilistic filter which can detect this, and
  when this setting is enabled events considered duplicates will be discarded without further mapping or processing.
:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.incoming_request_processor {
      discard_duplicates = true
    }

divolte.kafka_flusher
---------------------
This section controls settings related to forwarding the event stream to a Apache Kafka topic. Events for Kafka topics
are keyed by their party identifier.

divolte.kafka_flusher.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This controls whether flushing to Kafka is enabled or not. (This is disabled by default because the producer configuration for Kafka is normally site-specific.)
:Default:
  ``false``
:Example:

  .. code-block:: none

    divolte.kafka_flusher {
      enabled = true
    }

divolte.kafka_flusher.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for flushing events to Kafka.
:Default:
  ``2``
:Example:

  .. code-block:: none

    divolte.kafka_flusher {
      threads = 1
    }

divolte.kafka_flusher.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum number of mapped events to queue internally *per thread* for Kafka before starting to drop them.
:Default:
  ``200000``
:Example:

  .. code-block:: none

    divolte.kafka_flusher {
      max_write_queue = 1000000
    }

divolte.kafka_flusher.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum time to wait before dropping the event if the internal queue for one of the Kafka threads is full.
:Default:
  1 second
:Example:

  .. code-block:: none

    divolte.kafka_flusher {
      max_enqueue_delay = 20 seconds
    }

divolte.kafka_flusher.topic
^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The Kafka topic onto which events are published.
:Default:
  ``divolte``
:Example:

  .. code-block:: none

    divolte.kafka_flusher {
      topic = clickevents
    }

divolte.kafka_flusher.producer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The producer configuration. All settings are used as-is to configure the Kafka producer; refer to the `Kafka Documentation <http://kafka.apache.org/082/documentation.html#newproducerconfigs>`_ for further details.
:Default:

  .. code-block:: none

    producer = {
      bootstrap.servers = "localhost:9092"
      bootstrap.servers = ${?DIVOLTE_KAFKA_BROKER_LIST}

      client.id = divolte.collector
      client.id = ${?DIVOLTE_KAFKA_CLIENT_ID}

      acks = 0
      retries = 5
      retry.backoff.ms = 200
    }

:Example:

  .. code-block:: none

    divolte.kafka_flusher.producer = {
      metadata.broker.list = ["broker1:9092", "broker2:9092", "broker3:9092"]
      client.id = divolte.collector

      request.required.acks = 0
      message.send.max.retries = 5
      retry.backoff.ms = 200
    }

divolte.hdfs_flusher
--------------------
This section controls settings related to flushing the event stream.

divolte.hdfs_flusher.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This controls whether flushing to HDFS is enabled. Note that in absence of further HDFS configuration events will be written to the local filesystem.
:Default:
  ``true``
:Example:

  .. code-block:: none

    divolte.hdfs_flusher {
      enabled = false
    }

divolte.hdfs_flusher.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for flushing events to HDFS. Each thread creates its own files on HDFS. Depending on the flushing strategy, multiple concurrent files can be kept open per thread.
:Default:
  ``2``
:Example:

  .. code-block:: none

    divolte.hdfs_flusher {
      threads = 1
    }

divolte.hdfs_flusher.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum number of mapped events to queue internally *per thread* for HDFS before starting to drop them.
:Default:
  100000
:Example:

  .. code-block:: none

    divolte.hdfs_flusher {
      max_write_queue = 1000000
    }

divolte.hdfs_flusher.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum time to wait before dropping the event if the internal queue for one of the HDFS threads is full.
:Default:
  1 second
:Example:

  .. code-block:: none

    divolte.hdfs_flusher {
      max_enqueue_delay = 20 seconds
    }

divolte.hdfs_flusher.hdfs
-------------------------
HDFS specific settings. Although it is possible to configure a HDFS URI here, it is more advisable to configure HDFS settings by specifying a ``HADOOP_CONF_DIR`` environment variable which will be added to the classpath on startup.

divolte.hdfs_flusher.hdfs.uri
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The filesystem URI to configure the HDFS client with. When absent, the URI is not set. When using ``HADOOP_CONF_DIR`` this should not be set.
:Default:
  *Not set*
:Example:

  .. code-block:: none

    divolte.hdfs_flusher.hdfs {
      uri = "file:///"
    }

divolte.hdfs_flusher.hdfs.replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The HDFS replication factor to use when creating files.
:Default:
  ``1``
:Example:

  .. code-block:: none

    divolte.hdfs_flusher.hdfs {
      replication = 3
    }

divolte.hdfs.file_strategy
--------------------------
Divolte Collector has two strategies for creating files on HDFS and flushing data. One of these must be configured, but not both. Which strategy to use is set using the `type` property of this configuration; accepted values are either ``SIMPLE_ROLLING_FILE` (default) or ``SESSION_BINNING``.

Simple rolling file strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default a simple rolling file strategy is employed. This opens one file per thread and rolls over to a new file after a configurable interval. Files that are being written to have an extension of ``.avro.partial`` and are created in the the directory configured in the ``working_dir`` setting. When a file is closed, it is renamed to have an ``.avro`` extension and moved to the directory configured in the ``publish_dir`` setting. This happens in a single (atomic) filesystem move operation.

Session binning file strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A more complex strategy is the session binning strategy. This strategy attempts to place events that belong to the same session in the same file.

Events are assigned to files using the following rules:

- The strategy always has a 'current' open file to which events will be written.
- When a session starts, its events are assigned to the current file and will be written there for as long as possible.
- When a period of time the length of the configured session timeout has elapsed, a new file is opened and designed 'current'.
- The previously current file remains open for a further period of time equal to twice the session timeout. During this
  period events for sessions assigned to that file will be written there.
- If an event arrives assigned to file that has been closed, the session's events will be reassigned to the oldest open
  file.

.. note::

  If the Divolte Collector is shutdown or fails, open files are not moved into the published directory. Instead they
  remain in the working directory and need to be manually processed.

divolte.hdfs.file_strategy.type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Identify which strategy to use for flushing HDFS files. Type can be either `SIMPLE_ROLLING_FILE` or `SESSION_BINNING` for the respective strategies.
:Default:
  ``SIMPLE_ROLLING_FILE``
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      type = SESSION_BINNING
    }

divolte.hdfs.file_strategy.sync_file_after_records
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  How often a ``hsync()`` should be issued to flush HDFS data based on the number of records that have been written since the last flush.
:Default:
  ``1000``
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      sync_file_after_records = 100
    }

divolte.hdfs.file_strategy.sync_file_after_duration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  How often a ``hsync()`` should be issued to flush HDFS data based on how long it is since the last flush.
:Default:
  30 seconds
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      sync_file_after_duration = 1 minute
    }

divolte.hdfs.file_strategy.working_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are created and kept while being written to.
:Default:
  ``/tmp``
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      working_dir = /webdata/inflight
    }

divolte.hdfs.file_strategy.publish_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are moved to after they are closed.
:Default:
  ``/tmp``
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      publish_dir = /webdata/published
    }

divolte.hdfs.file_strategy.roll_every *(simple rolling strategy only)*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Roll over files on HDFS after this amount of time.
:Default:
  60 minutes
:Example:

  .. code-block:: none

    divolte.hdfs.file_strategy {
      roll_every = 15 minutes
    }

logback.xml
-----------
Divolte Collector uses the `Logback Project <http://logback.qos.ch>`_ as its logging provider. This provider is configured through the ``logback.xml`` file in the configuration directory. For more information about the settings in this file, review the `Configuration chapter in the Logback Manual <http://logback.qos.ch/manual/configuration.html>`_.

Website integration
===================
Next to the server side configuration, Divolte Collector needs to be integrated into a website in order to log events. The minimum integration involves adding a single tag to collect pageviews. This can be extended with custom events for tracking specific user interactions.

The tag
-------
The tag for Divolte Collector to include in each page of a website is this:

.. code-block:: html

  <script src="//track.example.com/divolte.js" defer async></script>

The URL will need to use the domain name where you are hosting Divolte Collector, and ``divolte.js`` needs to match the ``divolte.javascript.name`` configuration setting.

Custom events
-------------
The tracking tag provides an API for pages to fire custom events:

.. code-block:: html

  <script>
    divolte.signal('eventType', { 'foo': 'divolte', 'bar': 42 })
  </script>

The first argument to the ``divolte.signal(...)`` function is the event type parameter. The second argument is a arbitrary object with custom event parameters. Storing the event parameter and the custom event parameters into the configured Avro data is achieved through the mapping. See the :doc:`mapping_reference` chapter for details.
