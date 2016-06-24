.. Divolte Collector documentation master file.

Divolte Collector User Guide
############################
This is the user guide to for Divolte Collector. Please see our `GitHub repository <https://github.com/divolte/divolte-collector>`_ for developer documentation or source code.

Because life's too short for logfile parsing
--------------------------------------------
Divolte Collector consists of a JavaScript tag on the client side and a lightweight server for collecting click stream data to HDFS for offline processing and to Kafka queues for near real-time processing. Often, this problem is otherwise solved by parsing server log files or downloading click stream logs from third party solutions, such as Google Analytics or Omniture. Divolte Collector aims to solve the problem of collecting click stream data without depending on such third parties and keeping full control over your own data and ownership. Please see the :doc:`introduction` for a more complete overview.

Contents
========

.. toctree::
   :maxdepth: 2

   introduction
   getting_started
   configuration
   mapping_reference
   deployment

Indices and tables
==================

* :ref:`genindex`
