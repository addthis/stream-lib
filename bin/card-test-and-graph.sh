#!/bin/bash -x

# Wrap the maven boilerplate to run the cardinality tests and graph results.

mvn -e exec:java -Dexec.classpathScope="test"  -Dexec.mainClass="com.clearspring.analytics.stream.cardinality.TestAndGraphResults"  -Dexec.args="$*"
