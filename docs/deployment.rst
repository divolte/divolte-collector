**********
Deployment
**********
This chapter describes common steps for deploying Divolte Collector in production.

Installation/packages
=====================
The distributions provided for Divolte Collector are:

- A ``.tar.gz`` archive distribution containing the binaries and startup scripts.
- A ``.zip`` archive distribution containing the binaries and startup scripts.
- A RPM that can be installed onto Red Hat/CentOS systems. This includes startup and init scripts.

Currently there is no Debian packaging.

Load balancers
==============
In a production scenario, Divolte Collector is typically deployed behind a load balancer, both for fail-over and load balancing. For high availability, users need to ensure that the load balancing infrastructure is highly available. This can be achieved by specialized hardware load balancers or through DNS load balancing. The exact implementation of these are beyond the scope of this document.

Divolte Collector is semi-stateless. This means that it is not required that requests form the same client always go to the same instance; the event will be logged in all cases. Divolte Collector does however build up some soft state during operation for detecting duplicate events and caching parsed user agents. This means that there is benefit in stickyness, but it is not a requirement.

URI/hash-based load balancing policy
------------------------------------
Divolte Collector keeps a short term memory for detecting duplicate requests. In order for this to work, exact duplicate requests need to always go to the same instance. Most load balancers can support this by setting up a routing policy that uses a hash of the requested URI to determine which instance to route the request to. When using duplicate detection, be sure to configure your load balancer to do this.

Consistent hashing and event de-duplication
-------------------------------------------
If possible, load balancers should use a consistent hashing scheme when performing URI hash-based routing. This should ensure that most traffic continues to be routed to the same instance as before. The benefit of this is that the duplicate memory kept by Divolte Collector nodes remains effective.

SSL
===
Divolte Collector does not handle SSL itself. SSL offloading needs to be done by a load balancer or a reverse proxy server. This can normally handled by the load balancer in front of Divolte Collector in production setups.

Example nginx configuration
===========================
When using `nginx <http://nginx.org/>`_ as a reverse proxy and load balancer in front of Divolte Collector, you can use this snippet for configuring nginx:

.. code-block:: nginx

  upstream divolte {
      hash $request_uri consistent;

      server divolte1.internaldomain:8290;
      server divolte1.internaldomain:8290;
  }

  server {
      listen       80;
      server_name  tracking.example.com;

      location / {
          proxy_pass              http://divolte;
          proxy_redirect          off;
          proxy_buffering         off;
          proxy_set_header        Host $http_host;
          proxy_set_header        X-Real-IP $remote_addr;
          proxy_set_header        X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header        X-Forwarded-Proto $scheme;
          proxy_pass_header       Server;
      }

      # redirect server error pages to the static page /50x.html
      #
      error_page   500 502 503 504  /50x.html;
      location = /50x.html {
          root   /usr/share/nginx/html;
      }
  }

  server {
      listen       443 ssl;
      server_name  tracking.example.com;

      location / {
          proxy_pass              http://divolte;
          proxy_redirect          off;
          proxy_buffering         off;
          proxy_set_header        Host $http_host;
          proxy_set_header        X-Real-IP $remote_addr;
          proxy_set_header        X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header        X-Forwarded-Proto $scheme;
          proxy_pass_header       Server;
      }

      ssl_certificate      /etc/nginx/star.example.com.pem;
      ssl_certificate_key  /etc/nginx/star.example.com.key;

      ssl_session_cache shared:SSL:1m;
      ssl_session_timeout  5m;

      ssl_ciphers  HIGH:!aNULL:!MD5;
      ssl_prefer_server_ciphers   on;

      # redirect server error pages to the static page /50x.html
      #
      error_page   500 502 503 504  /50x.html;
      location = /50x.html {
          root   /usr/share/nginx/html;
      }
  }

Kafka Connect
=============
When deploying in conjunction with Kafka Connect, the Avro schemas need to be pre-registered with the `Schema Registry <https://docs.confluent.io/3.3.0/schema-registry/docs>`_. Mappings that produce records for a Kafka sink operating in ``confluent`` mode need have their ``confluent_id`` property configured with the identifier of the schema in the registry. (This identifier is normally a simple integer.)

Service endpoint
================

To support rolling updates and scaling without losing data, Divolte exposes an endpoint that allows graceful shutdown of the docker container. Currently the `/ping` endpoint is tested on Kubernetes. The main issue with Kubernetes is, even it has send a signal to stop the container, the container will remain in the pool of the load balancer. This will cause the divolte container to receive new connections, which is not something that we want when initializing a graceful shutdown procedure. Instead Divolte should clear all the buffers by flushing them to persistent storage. The sequence:

- Kubernetes sends a SIGTERM to Divolte to let it know that it wants to shutdown the container.
- Making the container unhealthy by replying with HTTP 503 over the `/ping` endpoint, which will cause the container to be removed from the pool of the load balancer.
- The `divolte.global.server.shutdown_wait_period_mills` property should be at least twice the kubernetes liveness probe interval (10 seconds by default).
- The container is removed from the loadbalancer pool and divolte will wait `divolte.global.server.shutdown_grace_period_mills` to handle and close the open connections.
- Undertow will shut down, and all the sinks are flushed and closed.
- If everything is not handled within 30 seconds (by default), kubernetes will kill the container and data might be lost forever :(
