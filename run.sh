#!/bin/bash

set -eo pipefail

# Clear this so that ES doesn't repeatedly complain about ignoring it
export JAVA_HOME=''

AGENT_VERSION=$(awk '/apm_agent/ { print $3 }' build-tools-internal/version.properties)

# This is the path that `./gradlew localDistro` prints out at the end
cd build/distribution/local/elasticsearch-8.3.0-SNAPSHOT

# URL and token for sending traces
SERVER_URL=""
SECRET_TOKEN=""

# Configure the ES keystore, so that we can use `elastic:password` for REST
# requests
if [[ ! -f config/elasticsearch.keystore ]]; then
  ./bin/elasticsearch-keystore create
  echo "password" | ./bin/elasticsearch-keystore add -x 'bootstrap.password'
fi


# Optional - override the agent jar
# OVERRIDE_AGENT_JAR="$HOME/.m2/repository/co/elastic/apm/elastic-apm-agent/1.30.2-SNAPSHOT/elastic-apm-agent-1.30.2-SNAPSHOT.jar"

if [[ -n "$OVERRIDE_AGENT_JAR" ]]; then
  # Copy in WIP agent
  cp "$OVERRIDE_AGENT_JAR" "modules/apm-integration/elastic-apm-agent-${AGENT_VERSION}.jar"
fi

# Configure the agent
#   1. Enable the agent
#   2. Set the server URL
#   3. Set the secret token
perl -p -i -e " s|enabled: false|enabled: true| ; s|# server_url.*|server_url: $SERVER_URL| ; s|# secret_token.*|secret_token: $SECRET_TOKEN|" config/elasticapm.properties


# export ES_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=*:5007 "
# export ES_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5007 "

# export ES_JAVA_OPTS="-Djava.security.debug=failure"
# export ES_JAVA_OPTS="-Djava.security.debug=access,failure"

exec ./bin/elasticsearch -Expack.apm.tracing.enabled=true -Eingest.geoip.downloader.enabled=false
