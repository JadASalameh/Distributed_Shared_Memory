#!/bin/bash
# run-node.sh
# Usage: ./run-node.sh <NodeName>

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <NodeName>"
    exit 1
fi

NODE_NAME=$1
JAR_NAME="target/Distributed_Shared_Memory-1.0-SNAPSHOT.jar"

echo "Starting NodeMain for node: $NODE_NAME"
java -cp "$JAR_NAME":. NodeMain "$NODE_NAME"

