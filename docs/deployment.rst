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
