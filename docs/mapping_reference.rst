*******
Mapping
*******

Mapping in Divolte Collector is the definition that determines how incoming requests are translated into Avro records with a given schema. This definition is composed in a special, buit in `Groovy <http://groovy.codehaus.org/>`_ based DSL (domain specific language).

Why mapping?
============
Most clickstream data collection services or solutions use a canonical data model that is specific to click events and related properties. Things such as location, referer, remote IP address, path, etc. are all properties of a click event that come to mind. While Divolte Collector exposes all of these fields just as well, it is our vision that this is not enough to make it easy to build online and near real-time data driven products within specific domains and environments. For example, when working on a system for product recommendation, the notion of a URL or path for a specific page is completely in the wrong domain; what you would care about in this case is likely a product ID and probably a type of interaction (e.g. product page view, large product photo view, add to basket, etc.). It is usually possible to extract these pieces of information from the clickstream representation, which means custom parsers have to be created to parse this information out of URLs, custom events from JavaScript and other sources. This means that whenever you work with the clickstream data, you have to run these custom parsers initially in order to get meaninful, domain specific information from the data. When building real-time systems, it normally means that this parser has to run in multiple locations: as part of the off line processing jobs and as part of the real-time processing.

With Divolte Collector, instead of writing parsers and working with the raw clickstream event data in your processing, you define a mapping that allows Divolte Collector to do all the required parsing on the fly as events come in and subsequently produce structured records with a schema to use in further processing. This means that all data that comes in can already have the relevant domain specific fields populated. And whenever the need for a new extracted piece of information arises, you can update the mapping to include the new field in the newly produced data. The older data that lacks newly additional fields can co-exist with newer data that does have the additional fields through a process called schema evolution. This is supported by Avro's ability to read data with a different schema from the one that the data was written with.

In essence, the goal of the mapping is to get rid of log file or URL parsing on collected data after it is published. The event stream from Divolte Collector should have all the domain specific fields to support you use cases directly.

Understanding the mapping process
---------------------------------
Before you dive in to creating your own mappings, it is important to understand a little bit about how the mapping is actually performed. **The most notable thing to keep in mind is that the mapping script that you provide, is not evaluated at request time for each request.** Rather, it is evaluated only once on startup and the result of the script is used to perform the actual mapping. This means that your mapping script is evaluated only once during the run-time of the Divolte Collector server.

.. image:: images/mapping-request-run-time.png

Built in default mapping
------------------------
Divolte Collector comes with a built in default schema and mapping. This will map pretty much all of the basics that you would expect from a clickstream data collector. The Avro schema that is used can be found in the `divolte-schema Github repository <https://github.com/divolte/divolte-schema>`_. The following mappings are present in the default mapping:

===============================  =================
Mapped value                     Avro schema field
===============================  =================
`duplicate`_                     detectedDuplicate
`corrupt`_                       detectedCorruption
`firstInSession`_                firstInSession
`timestamp`_                     timestamp
`remoteHost`_                    remoteHost
`referer`_                       referer
`location`_                      location
`viewportPixelWidth`_            viewportPixelWidth
`viewportPixelHeight`_           viewportPixelHeight
`screenPixelWidth`_              screenPixelWidth
`screenPixelHeight`_             screenPixelHeight
`partyId`_                       partyId
`sessionId`_                     sessionId
`pageViewId`_                    pageViewId
`eventType`_                     eventType
`userAgentString`_               userAgentString
`User agent name`_               userAgentName
`User agent family`_             userAgentFamily
`User agent vendor`_             userAgentVendor
`User agent type`_               userAgentType
`User agent version`_            userAgentVersion
`User agent device category`_    userAgentDeviceCategory
`User agent OS family`_          userAgentOsFamily
`User agent OS version`_         userAgentOsVersion
`User agent OS vendor`_          userAgentOsVendor
===============================  =================

The default schema is not available as a mapping script. Instead, it is hard coded into Divolte Collector. This way, you can setup Divolte Collector to do something useful out-of-the-box without any complex configuration.

A word on schema evolution
--------------------------
Schema evolution is the process of changing the schema over time as requirements change. For example when a new feature is added to your website, you add additional fields to the schema that contain specific information about user interactions with this new feature. In this scenario, you would update the schema to have these additional fields, update the mapping and then run Divolte Collector with the new schema and mapping. This means that there will be a difference between data that was written prior to the update and data that is written after the update. Also, it means that after the update, there can still be consumers of the data (from HDFS or Kafka) that still use the old schema. In order to make sure that this isn't a problem, the readers with the old schema need to be able to read data written with the new schema and readers with the new schema should also still work on data written with the old schema.

Luckily, Avro supports both of these cases. When reading newer data with an older schema, the fields that are not present in the old schema are simply ignored by the reader. The other way araound is slightly trickier. When reading older data with a new schema, Avro will fill in the default values for fields that are present in the schema but not in the data. *This is provided that there is a default value.* Basically, this means that it is recommended to always provide a default value for all your fields in the schema. In case of nullable fields, the default value could just be null.

One other reason to always provide a default value is that Avro does not allow to create records with missing values if there are no default values. As a result of this, fields that have no default value always must be populated in the mapping, otherwise an error will occur. This is problematic if the mapping for some reason fails to set a field (e.g. because of a user typing in a non-conforming location in the browser).

Mapping DSL
===========

Introduction
------------

A word on groovy
----------------

Constructs
----------

Mapping values onto fields (map)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Value presence and nulls
""""""""""""""""""""""""

Types
^^^^^

Casting
"""""""

Conditional mapping (when)
^^^^^^^^^^^^^^^^^^^^^^^^^^

Sections and short circuit
^^^^^^^^^^^^^^^^^^^^^^^^^^

exit
""""

stop
""""

Simple values
^^^^^^^^^^^^^

location
""""""""

referer
"""""""

firstInSession
""""""""""""""

corrupt
"""""""
duplicate
"""""""""

timestamp
"""""""""

remoteHost
""""""""""

viewportPixelWidth
""""""""""""""""""

viewportPixelHeight
"""""""""""""""""""

screenPixelWidth
""""""""""""""""

screenPixelHeight
"""""""""""""""""

devicePixelRatio
""""""""""""""""

partyId
"""""""

sessionId
"""""""""

pageViewId
""""""""""

eventId
"""""""

userAgentString
"""""""""""""""

cookie
""""""

eventType
"""""""""

eventParameter
""""""""""""""

Complex values
^^^^^^^^^^^^^^

Regular expression matching
"""""""""""""""""""""""""""

URI
"""

Query strings
"""""""""""""

User agent parsing
""""""""""""""""""

User agent name
~~~~~~~~~~~~~~~

User agent family
~~~~~~~~~~~~~~~~~

User agent vendor
~~~~~~~~~~~~~~~~~

User agent type
~~~~~~~~~~~~~~~

User agent version
~~~~~~~~~~~~~~~~~~

User agent device category
~~~~~~~~~~~~~~~~~~~~~~~~~~

User agent OS family
~~~~~~~~~~~~~~~~~~~~

User agent OS version
~~~~~~~~~~~~~~~~~~~~~

User agent OS vendor
~~~~~~~~~~~~~~~~~~~~

ip2geo
""""""

HTTP headers
""""""""""""

Examples
--------
