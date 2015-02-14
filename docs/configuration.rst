*************
Configuration
*************
This chapter describes the configuration mechanisms and available options for Divolte Collector.

Configuration files
===================
The configuration for Divolte Collector consists of three files:

- divolte-env.sh: a shell script that is included in the startup script to set environment variables and JVM startup arguments.
- divolte-collector.conf: the main configuration file for Divolte Collector.
- logback.xml: the logging configuration.

Configuration directory
-----------------------
Divolte Collector will try to find configuration files at startup in the configuration directory. Typically this is the /conf directory nested under the Divolte Collector installation. Divolte Collector will try to locate the configuration directory at ../conf relative to the startup script. The configuration directory can be overridden by setting the DIVOLTE_CONF_DIR environment variable. If set the value will be used as configuration directory. If you have installed Divolte Collector from a RPM, the init script will set this variable to /etc/divolte-collector.

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
  The directory where the JRE/JDK is located. Divolte Collector will uses $JAVA_HOME/bin/java as Java executable for startup. If this is not set, Divolte Collector will attempt to find a suitable JDK in a number of common Java installation locations on Linux systems. It is however not recommended to rely on this mechanism for production use.

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
This is the main configuration file for Divolte Collector. For configuration, Divolte Collector uses the `Typesafe Config library <https://github.com/typesafehub/config>`. The dialect of the configuration file is a JSON superset called HOCON (for *Human-Optimized Config Object Notation*). HOCON has a nested structure, like JSON, but is slightly less verbose and doesn't require escaping and quoting of strings in many cases. Here we outline some basic features of HOCON.

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
The following sections and settings are available in the divolte-collector.conf file. Nota that in this documentation the path notation for configuration options is used (e.g. divolte.server), but in examples the path and nested notation is used interchangeably.

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
  Whether to use the X-Forwarded-For header HTTP header for determining the source IP of a request if present. When a X-Forwared-For header is present, the rightmost IP address of the value is used as source IP when when multiple IP addresses are separated by a comma. When the header is present more than once, the last value will be used.

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
This section controls the tracking mechanism for Divolte Collector, such as the cookies and session timeouts.

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
  The cookie domain that is assigned to the cookies. When left empty, the cookie will have no domain explicitly associated with it, which effectively  sets it to the website domain of the page that contains the Divolte Collector JavaScript.
:Default:
  '' (empty)
:Example:

  ::

    divolte.tracking {
      cookie_domain = '.example.com'
    }

divolte.tracking.ua_parser
--------------------------
This section controls the settings for the built in user agent string parser in Divolte Collector.

divolte.tracking.ua_parser.type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:Description:
  This section controls the user agent parsing settings. The user agent parsing is based on this library (https://github.com/before/uadetector), which allows for dynamic reloading of the backing database if a internet connection is available. The parser type controls this behavior.


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
----------------------------

divolte.tracking.schema_mapping
-------------------------------

divolte.javascript
------------------

divolte.incoming_request_processor
----------------------------------

divolte.kafka_flusher
---------------------

divolte.hdfs_flusher
--------------------

divolte.hdfs_flusher.hdfs
-------------------------

divolte.hdfs.simple_rolling_file_strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

divolte.hdfs.session_binning_file_strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Website integration
===================

The tag
-------

Custom events
-------------
