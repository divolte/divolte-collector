============
Introduction
============

---------------------------------------------------------------
Divolte Collector: Because life's too short for logfile parsing
---------------------------------------------------------------

The problem
===========
Analyzing your clickstream data always starts with the gathering of data. The easiest way to start collecting data about what users do on your website is getting the webservers access logs into an environment suitable to do analytics.

The next step to get value out of these logs is to parse the logfile format into something more suitable for analytics. The problem here is that the logs contain the full url's and you need to extract the usefull parts out of that url.
every time the url structure of your site changes you need to create a new iteration of your parser logic to cope with the new as well as with the old structure.

Getting your clickstream data into the analytics environment (Hadoop for example) is one thing and gives you the ability to get some value out of your data. But it's still a batch oriented process. So you need a seperate stream to get realtime information from your data. 
Most of the time tools like Flume or syslog-ng are used to get a seperate stream of realtime clickstream data. This second flow again needs to be parsed by the same parser logic.


Solving the problem
-------------------
Divolte Collector is created to solve the mentioned problem. 
In the image below the red circle points out the part of the datapipeline that Divolte Collector covers:

.. image:: images/divolte-part-of-process.png
   :alt: Datapipeline coverage of Divolte Collector


Divolte Collector: The parts
============================
Divolte Collector consists basically of the following parts:

I. The Javascript tag
II. The domain specific mapping
III. Data collection into Hadoop
IV. Data collection for realtime processing

I. The Javascript tag
---------------------
Tagging (or `Web bug <http://en.wikipedia.org/wiki/Web_bug>`__) is the name for instrumenting your webpages, by adding a piece of Javascript code or a special image, to enable analytics. (Think Google Analytics, Omniture etc.)

II. The domain specific mapping
-------------------------------
Here is the introduction…

III. Data collection into Hadoop
--------------------------------
Here is the introduction…

IV. Data collection for realtime processing
-------------------------------------------
Here is the introduction…

