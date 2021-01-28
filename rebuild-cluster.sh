#!/usr/bin/env bash

#
# Rebuild the local docker-compose cluster with a new version of the plugin
#

# Recompile the plugin
mvn clean package -DskipTests=true

# Tear down current cluster and remove the volumes
docker-compose -f docker-compose.cluster.yml down
docker volume ls -f dangling=true -q |  xargs docker volume rm

# Bring up a new cluster
docker-compose -f docker-compose.cluster.yml up
