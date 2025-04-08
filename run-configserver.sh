#!/bin/bash
# run-configserver.sh
# This script starts the ConfigServer from the default package.

# Adjust the path to the JAR name if necessary.
JAR_NAME="target/Distributed_Shared_Memory-1.0-SNAPSHOT.jar"

echo "Starting ConfigServer..."
java -cp "$JAR_NAME":. ConfigServer

