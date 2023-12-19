#!/bin/bash

set -e

echo "/etc/hosts"
echo $(cat "/etc/hosts")
addprinc.sh "elasticsearch"
addprinc.sh "hdfs/hdfs.build.elastic.co"

# Use this as a signal that setup is complete
python3 -m http.server 4444 &

sleep infinity
