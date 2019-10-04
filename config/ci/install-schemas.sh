#!/bin/sh

echo "Install Apache Maven"

curl -L https://archive.apache.org/dist/maven/maven-3/3.6.2/binaries/apache-maven-3.6.2-bin.tar.gz |
  tar -xzf - -C /usr/local &&
  ln -s /usr/local/apache-maven-3.6.2 /usr/local/apache-maven &&
  ln -s /usr/local/apache-maven/bin/mvn /usr/local/bin/

echo "Install the Divolte schema"

git clone --depth 1 https://github.com/divolte/divolte-schema.git /tmp/divolte-schema \
      && cd /tmp/divolte-schema \
      && mvn -q -B install
