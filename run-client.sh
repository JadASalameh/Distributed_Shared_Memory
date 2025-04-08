#!/bin/bash
# run-client.sh
# Usage: ./run-client.sh <input_file>

JAR_NAME="target/Distributed_Shared_Memory-1.0-SNAPSHOT.jar"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <input_file>"
  exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="client_output.txt"

echo "Starting batch Client using input file: $INPUT_FILE"
java -cp "$JAR_NAME":. Client "$INPUT_FILE" > "$OUTPUT_FILE"


