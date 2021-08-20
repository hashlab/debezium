#!/bin/bash
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

RUNNER=$(ls debezium-server-*runner.jar)

export JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:8000
export GOOGLE_APPLICATION_CREDENTIALS=conf/pubsub-publisher.json
exec $JAVA_BINARY $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER:conf:lib/*" io.debezium.server.Main
