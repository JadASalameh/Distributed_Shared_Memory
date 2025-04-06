#!/bin/bash
# Usage: ./addNode.sh <NodeName>
if [ "$#" -ne 1 ]; then
    echo "Usage: ./addNode.sh <NodeName>"
    exit 1
fi

NODE_NAME=$1

# Run the fat jar, passing the node name as an argument.
java -jar target/Distributed_Shared_Memory-1.0-SNAPSHOT.jar "$NODE_NAME"
