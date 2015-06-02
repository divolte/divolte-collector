*************
Configuration
*************
This chapter describes the configuration mechanisms and available options for Divolte Collector. If you are completely new to Divolte Collector, it is probably a good idea to go through the :doc:`getting_started` guide first.

Configuration files
===================
The configuration for Divolte Collector consists of three files:

- divolte-env.sh: a shell script that is included in the startup script to set environment variables and JVM startup arguments.
- divolte-collector.conf: the main configuration file for Divolte Collector.
- logback.xml: the logging configuration.

Configuration directory
-----------------------
Divolte Collector will try to find configuration files at startup in the configuration directory. Typically this is the /conf directory nested under the Divolte Collector installation. Divolte Collector will try to locate the configuration directory at ../conf relative to the startup script. The configuration directory can be overridden by setting the DIVOLTE_CONF_DIR environment variable. If set, the value will be used as configuration directory. If you have installed Divolte Collector from a RPM, the init script will set this variable to /etc/divolte-collector.

divolte-env.sh
--------------
This shell script is run by the startup script prior to starting the Divolte Collector JVM. The following environment variables influence Divolte Collector.

HADOOP_CONF_DIR
^^^^^^^^^^^^^^^
:Description:
  Directory where Hadoop / HDFS configuration files are to be found. This directory is included in the classpath on startup, which causes the HDFS client to load the configuration files.

:Example:

  ::

    HADOOP_CONF_DIR=/etc/hadoop/conf

JAVA_HOME
^^^^^^^^^
:Description:
  The directory where the JRE/JDK is located. Divolte Collector will use $JAVA_HOME/bin/java as Java executable for startup. If this is not set, Divolte Collector will attempt to find a suitable JDK in a number of common Java installation locations on Linux systems. It is however not recommended to rely on this mechanism for production use.

:Example:

  ::

    JAVA_HOME=/usr/java/default

DIVOLTE_JAVA_OPTS
^^^^^^^^^^^^^^^^^
:Description:
  Additional arguments passed to the Java Virtual Machine on startup. If not set, by default Divolte Collector will start the JVM with "-XX:+UseG1GC -Djava.awt.headless=true". It is recommend to use the G1 garbage collector. For light and medium traffic, the deaults tend to work fine. *If this setting is set, Divolte Collector will not add any arguments by itself; this setting overrides the defaults.*

:Example:

  ::

    DIVOLTE_JAVA_OPTS="-XX:+UseG1GC -Djava.awt.headless=true -XX:+HeapDumpOnOutOfMemoryError"

divolte-collector.conf
----------------------
This is the main configuration file for Divolte Collector. For configuration, Divolte Collector uses the `Typesafe Config library <https://github.com/typesafehub/config>`_. The dialect of the configuration file is a JSON superset called HOCON (for *Human-Optimized Config Object Notation*). HOCON has a nested structure, like JSON, but is slightly less verbose and doesn't require escaping and quoting of strings in many cases. Here we outline some basic features of HOCON.

Nesting and dot separated namespacing can be used interchangeably::

  // This:
  divolte {
    server {
      host = 127.0.0.1
    }
  }

  // Is the same as this:
  divolte.server.host = 127.0.0.1

Environment variable overrides can be used. In this example the divolte.server.port setting defaults to 8290, unless the DIVOLTE_PORT environment veriable is set::

  divolte {
    server {
      port = 8290
      port = ${?DIVOLTE_PORT}
    }
  }

Objects are merged::

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

For a full overview of the HOCON features and specification, see here: https://github.com/typesafehub/config/blob/master/HOCON.md

Configuration reference
=======================
The following sections and settings are available in the divolte-collector.conf file. Note that in this documentation the path notation for configuration options is used (e.g. divolte.server), but in examples the path and nested notation is used interchangeably.

divolte.server
--------------
This sections controls the settings for the internal HTTP server of Divolte Collector.

divolte.server.host
^^^^^^^^^^^^^^^^^^^
:Description:
  The host to which the server binds. Set to a specific IP address to selectively listen on that interface.
:Default:
  0.0.0.0
:Example:
  
  ::

    divolte.server {
      host = 0.0.0.0
    }

divolte.server.port
^^^^^^^^^^^^^^^^^^^
:Description:
  The TCP port on which the sever listens.
:Default:
  8290
:Example:

  ::

    divolte.server {
      port = 8290
    }

divolte.server.use_x_forwarded_for
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Whether to use the X-Forwarded-For header HTTP header for determining the source IP of a request, if present. When set to true and a X-Forwared-For header is present, the rightmost IP address of the value is used as source IP when multiple IP addresses are separated by a comma. When the header is present more than once, the last value will be used.

  | E.g.:
  | "X-Forwarded-For: 10.200.13.28, 11.45.82.30" ==> 11.45.82.30
  | 
  | "X-Forwarded-For: 10.200.13.28"
  | "X-Forwarded-For: 11.45.82.30" ==> 11.45.82.30

:Default:
  false
:Example:

  ::

    divolte.server {
      use_x_forwarded_for = true
    }

divolte.server.serve_static_resources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When true Divolte Collector serves a static test page at /.
:Default:
  true
:Example:

  ::

    divolte.server {
      serve_static_resources = false
    }

divolte.tracking
----------------
This section controls the tracking mechanism for Divolte Collector, such as the cookies and session timeouts, user agent parsing and ip2geo lookups.

divolte.tracking.party_cookie
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The name of the cookie used for setting a party ID.
:Default:
  _dvp
:Example:

  ::

    divolte.tracking {
      party_cookie = _pid
    }

divolte.tracking.party_timeout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The expiry time for the party ID cookie.
:Default:
  730 days
:Example:

  ::

    divolte.tracking {
      party_timeout = 1000 days
    }

divolte.tracking.session_cookie
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The name of the cookie used tracking the session ID.
:Default:
  _dvs
:Example:

  ::

    divolte.tracking {
      session_cookie = _sid
    }

divolte.tracking.session_timeout
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The expiry time for a session.
:Default:
  30 minutes
:Example:

  ::

    divolte.tracking {
      session_timeout = 1 hour
    }

divolte.tracking.cookie_domain
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The cookie domain that is assigned to the cookies. When left empty, the cookies will have no domain explicitly associated with them, which effectively sets it to the website domain of the page that contains the Divolte Collector JavaScript.
:Default:
  '' (empty)
:Example:

  ::

    divolte.tracking {
      cookie_domain = '.example.com'
    }

divolte.tracking.ip2geo_database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This configures the ip2geo database for geo lookups. A ip2geo database can be obtained from MaxMind (https://www.maxmind.com/en/geoip2-databases). Both a free version and a more accurate paid version are available.

  By default, no ip2geo database is configured. When this setting is absent, no attempt will be made to lookup geo-coordinates for IP addresses. If configured, Divolte Collector will keep a filesystem watch on the database file. If the file is changed on the filesystem the database will be reloaded at runtime without requireing a restart.
:Default:
  not set
:Example:

  ::

    divolte.tracking {
      ip2geo_database = '/etc/divolte/ip2geo/GeoLite2-City.mmdb'
    }

divolte.tracking.ua_parser
--------------------------
This section controls the user agent parsing settings. The user agent parsing is based on a open source parsing library (https://github.com/before/uadetector), which allows for dynamic reloading of the backing database if a internet connection is available.

divolte.tracking.ua_parser.type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This setting controls the updating behavior of the user agent parser.

  Possible values are:

  - non_updating:         Uses a local database, bundled with Divolte Collector.
  - online_updating:      Uses a online database only, never falls back to the local database.
  - caching_and_updating: Uses a cached version of the online database and periodically checks for new version at the remote location. Updates are downloaded automatically and cached locally.


  **Important: due to a change in the licensing of the user agent database, the online database for the user agent parser is no longer available.** In Divolte Collector versions 0.2 and below, leave this setting to 'non_updating'. In the next release, there will be a permanent solution to this problem.
:Default:
  non_updating
:Example:

  ::

    divolte.tracking.ua_parser {
       type = caching_and_updating
    }

divolte.tracking.ua_parser.cache_size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  User agent parsing is a relatively expensive operation that requires many regular expression evaluations. Very often the same user agent will make consecutive requests and many clients will have the exact same user agent as well. It therefore makes sense to cache the parsing results in memory and do a lookup before trying a parse. This setting determines how many unique user agent strings will be cached. 
:Default:
  1000
:Example:

  ::

    divolte.tracking.ua_parser {
      cache_size = 10000
    }

divolte.tracking.schema_file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  By default, Divolte Collector will use a built-in Avro schema for writing data and a default mapping, which is documented in the Mapping section of the user documentation. The default schema can be found here: https://github.com/divolte/divolte-schema

  Typically, users will configure their own schema, usually with fields specific to their domain and custom events and other mappings. When using a user defined schema, it is also required to provide a mapping script. See :doc:`mapping_reference` for further reference.
:Default:
  not set (uses built in schema)
:Example:

  ::

    divolte.tracking {
      schema_file = /etc/divolte/MyEventRecord.avsc
    }

divolte.tracking.schema_mapping
-------------------------------
This section controls the schema mapping to use. Schema mapping is an important feature of Divolte Collector, as it allows users to map incoming requests onto custom Avro schema's in non-trivial ways. See :doc:`mapping_reference` for details about this process and the internal mapping DSL used for defining mappings.

divolte.tracking.schema_mapping.version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Multiple versions of the mapping DSL can be supported by versions of Divolte Collector. Currently, the latest version is 2. Version 1 is deprecated and support for it will be removed. There is no documentation available for version 1. *For this version of Divolte Collector, always set this to 2.*
:Default:
  not set (uses built in mapping)
:Example:

  ::

    divolte.tracking.schema_mapping {
      version = 2
    }

divolte.tracking.schema_mapping.mapping_script_file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The groovy script file to use as mapping definition.
:Default:
  not set (uses built in mapping)
:Example:

  ::

    divolte.tracking.schema_mapping {
      mapping_script_file = /etc/divolte/my-mapping.groovy
    }

divolte.javascript
------------------
On startup, Divolte Collector internally compiles the JavaScript that it serves using `Google's Closure Compiler <https://developers.google.com/closure/compiler/>`_. This minifies the JavaScript and ensures there are no compilation errors or warnings. The javascript section controls settings related to the way the JavaScript file is compiled.

divolte.javascript.name
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The filename of the JavaScript that is served. This changes the divolte.js part in the script url: http://example.com/divolte.js.
:Default:
  divolte.js
:Example:

  ::

    divolte.javascript {
      name = tracking.js
    }

divolte.javascript.logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Enable or disable the logging on the JavaScript console in the browser.
:Default:
  false
:Example:

  ::

    divolte.javascript {
      logging = true
    }

divolte.javascript.debug
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When true, the served JavaScript will be compiled, but not minified, improving readability when debugging in the browser.
:Default:
  false
:Example:

  ::

    divolte.javascript {
      debug = true
    }

divolte.javascript.auto_page_view_event
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  When false, the served JavaScript will not automatically send a pageView event after being loaded. This way clients can send a initial event themselves and have full control over the event type and the custom parameters that are sent with the initial event.
:Default:
  true
:Example:

  ::

    divolte.javascript {
      auto_page_view_event = false
    }


divolte.incoming_request_processor
----------------------------------
This section controls settings related to the processing of incoming requests after they have been responded to by the server. Incoming requests in Divolte Collector are initially handled by a pool of HTTP threads, which immediately respond with a HTTP code 200 and send the response payload (a 1x1 pixel transparent GIF image). After responding, the request data is passed onto the incoming request processing thread pool. This is the incoming request processor.

divolte.incoming_request_processor.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for processing incoming requests.
:Default:
  2
:Example:

  ::

    divolte.incoming_request_processor {
      threads = 1
    }

divolte.incoming_request_processor.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum queue of incoming requests to keep before starting to drop incoming requests. Note that when this queue is full, requests are dropped and a warning is logged. No errors are reported to the client side. Divolte Collector will always respond with a HTTP 200 status code and the image payload. *Note that the queue size is per thread.*
:Default:
  100000
:Example:

  ::

    divolte.incoming_request_processor {
      max_write_queue = 1000000
    }

divolte.incoming_request_processor.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum delay to block before an incoming request is dropped in case of a full queue.
:Default:
  1 second
:Example:

  ::

    divolte.incoming_request_processor {
      max_enqueue_delay = 20 seconds
    }

divolte.incoming_request_processor.discard_corrupted
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The incoming request handler attempts to parse out all relevant information from the request as passed by the JavaScript. If the incoming request appears corrupt, for example because of a truncated URL or incorrect data in the fields, the request is flagged as corrupt. The detection of corrupt requests is enforced by appending a hash of all fields to the request from the JavaScript. This hash is validated on the server side. If this setting is true, events that are flagged as corrupt will be dropped from the stream, instead of being processed further. It is common not to drop the corrupt events, but instead include them for later analysis.
:Default:
  false
:Example:

  ::

    divolte.incoming_request_processor {
      discard_corrupted = true
    }

divolte.incoming_request_processor.duplicate_memory_size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Browsers and other clients (e.g. anti-virus software, proxies) will sometimes send the exact same request twice. Divolte Collector attempts to flag these duplicate events by using a internal probabilistic data structure with a fixed memory size. The memory consists internally of an array of 64 bit integers. This the memory required in bytes is the memory size times 8 (8 megabytes for 1 million entries). *Note that the memory size is per thread.*
:Default:
  1000000
:Example:

  ::

    divolte.incoming_request_processor {
      duplicate_memory_size = 10000000
    }

divolte.incoming_request_processor.discard_duplicates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  If this setting is true, events that are flagged as duplicate will be dropped from the stream, instead of being processed further. It is common not to drop the duplicate events, but instead include them for later analysis.
:Default:
  false
:Example:

  ::

    divolte.incoming_request_processor {
      discard_duplicates = true
    }

divolte.kafka_flusher
---------------------
This section controls settings related to flushing the event stream to a Apache Kafka topic.

divolte.kafka_flusher.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  If true, flushing to Kafka is enabled. Kafka flushing is disabled by default, as it is hard to set sensible defaults for the Kafka producer configuration.
:Default:
  false
:Example:

  ::

    divolte.kafka_flusher {
      enabled = true
    }

divolte.kafka_flusher.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for flushing events to Kafka.
:Default:
  2
:Example:

  ::

    divolte.kafka_flusher {
      threads = 1
    }

divolte.kafka_flusher.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum length of the queue of events to keep in the case that Kafka is unavailable before starting to drop incoming events. Note that when this queue is full, events are dropped and a warning is logged. No errors are reported to the client side. Divolte Collector will always respond with a HTTP 200 status code and the image payload. *Note that the queue size is per thread.*
:Default:
  200000
:Example:

  ::

    divolte.kafka_flusher {
      max_write_queue = 1000000
    }

divolte.kafka_flusher.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum delay to block before an incoming event is dropped in case of a full queue.
:Default:
  1 second
:Example:

  ::

    divolte.kafka_flusher {
      max_enqueue_delay = 20 seconds
    }

divolte.kafka_flusher.topic
^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The Kafka topic onto which events are published.
:Default:
  divolte
:Example:

  ::

    divolte.kafka_flusher {
      topic = clickevents
    }

divolte.kafka_flusher.producer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  All settings in here are used as-is to configure the Kafka producer. See: http://kafka.apache.org/documentation.html#producerconfigs
:Default:
  
  ::

    producer = {
      metadata.broker.list = ["localhost:9092"]
      metadata.broker.list = ${?DIVOLTE_KAFKA_BROKER_LIST}

      client.id = divolte.collector
      client.id = ${?DIVOLTE_KAFKA_CLIENT_ID}

      request.required.acks = 0
      message.send.max.retries = 5
      retry.backoff.ms = 200
    }

:Example:

  ::

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
  If true, flushing to HDFS is enabled. Flushing to HDFS is enabled by default. Note that in absence of further HDFS configuration, the HDFS client defaults to writing to the local filesystem.
:Default:
  true
:Example:

  ::

    divolte.hdfs_flusher {
      enabled = false
    }

divolte.hdfs_flusher.threads
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Number of threads to use for flushing events to HDFS. Each thread creates its own files on HDFS. Depending on the flushing strategy, multiple concurrent files could be kept open per thread.
:Default:
  2
:Example:

  ::

    divolte.hdfs_flusher {
      threads = 1
    }

divolte.hdfs_flusher.max_write_queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum length of the queue of events to keep in the case that HDFS is unavailable before starting to drop incoming events. Note that when this queue is full, events are dropped and a warning is logged. No errors are reported to the client side. Divolte Collector will always respond with a HTTP 200 status code and the image payload. *Note that the queue size is per thread.*
:Default:
  100000
:Example:

  ::

    divolte.hdfs_flusher {
      max_write_queue = 1000000
    }

divolte.hdfs_flusher.max_enqueue_delay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The maximum delay to block before an incoming event is dropped in case of a full queue.
:Default:
  1 second
:Example:

  ::

    divolte.hdfs_flusher {
      max_enqueue_delay = 20 seconds
    }

divolte.hdfs_flusher.hdfs
-------------------------
HDFS specific settings. Although it is possible to configure a HDFS URI here, it is more advisable to configure HDFS settings by specifying a HADOOP_CONF_DIR environment variable which will be added to the classpath on startup and as such configure the HDFS client automatically.

divolte.hdfs_flusher.hdfs.uri
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The filesystem URI to configure the HDFS client with. When absent, the URI is not set. In case of setting a HADOOP_CONF_DIR, this should not be set.
:Default:
  not set
:Example:

  ::

    divolte.hdfs_flusher.hdfs {
      uri = "file:///"
    }

divolte.hdfs_flusher.hdfs.replication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  The HDFS replication factor to use when creating files.
:Default:
  1
:Example:

  ::

    divolte.hdfs_flusher.hdfs {
      replication = 3
    }


divolte.hdfs.simple_rolling_file_strategy
-----------------------------------------
Divolte Collector has two strategies for creating files on HDFS and flushing data. Either one of these must be configured, but not both. By default, a simple rolling file strategy is employed. This opens one file per thread and rolls on to a new file after a configurable interval. Files that are being written to have a extension of .avro.partial and are created in the the directory configured in the working_dir setting. When a file is closed, it is renamed to have a .avro extension and is moved to the directory configured in the publish_dir setting. This happens in a single (atomic) filesystem move operation.

divolte.hdfs.simple_rolling_file_strategy.roll_every
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Roll over files on HDFS after this amount of time.
:Default:
  60 minutes
:Example:

  ::

    divolte.hdfs.simple_rolling_file_strategy {
      roll_every = 15 minutes
    }

divolte.hdfs.simple_rolling_file_strategy.sync_file_after_records
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Issue a hsync against files each time this number of records has been flushed to it.
:Default:
  1000
:Example:

  ::

    divolte.hdfs.simple_rolling_file_strategy {
      sync_file_after_records = 100
    }

divolte.hdfs.simple_rolling_file_strategy.sync_file_after_duration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Issue a hsync at least when this amount of time passes, regardless of how much data was written to a file.
:Default:
  30 seconds
:Example:

  ::

    divolte.hdfs.simple_rolling_file_strategy {
      sync_file_after_duration = 1 minute
    }

divolte.hdfs.simple_rolling_file_strategy.working_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are created and kept while being written to.
:Default:
  /tmp
:Example:

  ::

    divolte.hdfs.simple_rolling_file_strategy {
      working_dir = /webdata/inflight
    }

divolte.hdfs.simple_rolling_file_strategy.publish_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are moved to, after they are closed.
:Default:
  /tmp
:Example:

  ::

    divolte.hdfs.simple_rolling_file_strategy {
      publish_dir = /webdata/published
    }

divolte.hdfs.session_binning_file_strategy
------------------------------------------
Next to the rolling file strategy, there is a more complex strategy called session binning file strategy. The general idea of this strategy is to provide a best effort to put events that belong to the same session in the same file.

This strategy assigns events to files as such:

- Each event is assigned to a round based on timestamp, defined as timestamp_in_millis / session_timeout_in_millis.

- A file is opened for each round as time passes.

- All events for a session are stored in the file with the round marked by the session start time.

- A file for a round is kept open for at least three times the session duration *in absence of failures*.

- During this entire process, the event timestamp is used for events that come off the queue as a logical clock signal.

  * Only in the case of an empty queue, the actual system time is used as clock signal.

- When a file for a round is closed, but events that should be in that file still arrive, they are stored in the oldest open file.

  * This happens for exceptionally long sessions

This strategy attempts to write events that belong to the same session to the same file. Do note that in case of failures, this guarantee no longer holds. For this reason, in failure scenarios or at shutdown, this strategy DOES NOT move files to the publish directory. Users have to setup a separate process to periodically move these files out of the way. 

divolte.hdfs.session_binning_file_strategy.sync_file_after_records
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Issue a hsync against files each time this number of records has been flushed to it. 
:Default:
  1000
:Example:

  ::

    divolte.hdfs.session_binning_file_strategy {
      sync_file_after_records = 100
    }

divolte.hdfs.session_binning_file_strategy.sync_file_after_duration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Issue a hsync at least when this amount of time passes, regardless of how much data was written to a file.
:Default:
  30 seconds
:Example:

  ::

    divolte.hdfs.session_binning_file_strategy {
      sync_file_after_duration = 1 minute
    }

divolte.hdfs.session_binning_file_strategy.working_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are created and kept while being written to.
:Default:
  /tmp
:Example:

  ::

    divolte.hdfs.session_binning_file_strategy {
      working_dir = /webdata/inflight
    }

divolte.hdfs.session_binning_file_strategy.publish_dir
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  Directory where files are moved to, after they are closed.
:Default:
  /tmp
:Example:

  ::

    divolte.hdfs.session_binning_file_strategy {
      publish_dir = /webdata/published
    }

logback.xml
-----------
Divolte Collector uses the `Logback Project <http://logback.qos.ch>`_ as its logging provider. This provider is configured through the logback.xml file in the configuration directory. For more information about the settings in this file, review the `Configuration chapter in the Logback Manual <http://logback.qos.ch/manual/configuration.html>`_.

Website integration
===================
Next to the server side configuration, Divolte Collector needs to be integrated into a website in order to log events. The basic configuration consists of just adding a single tag to log pageviews, but in many cases it makes sense to add custom events for tracking specific user interactions.

The tag
-------
The tag for Divolte Collector to include in each page of a website is this::

  <script src="//track.example.com/divolte.js" defer async></script>

Change the domain name to the domain where you are hosting Divolte Collector. Also, if you configure a different script name than the default, the location will be different as well.

Custom events
-------------
In order to fire custom events from the browser, use the following JavaScript code::

  <script>
    divolte.signal('eventType', { 'foo': 'divolte', 'bar': 42 })
  </script>

The first argument to the divolte.signal(...) function is the event type parameter. The second argument is a arbitrary object with custom event parameters. Storing the event parameter and the custom event parameters into the configured Avro data is achieved through the mapping. See the :doc:`mapping_reference` chapter for details.
