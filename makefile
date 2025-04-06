# Makefile for Distributed Shared Memory Project

.PHONY: all clean run

# The "all" target compiles the project using Maven
all:
	mvn clean package

# The "run" target runs the project with a given node name.
# You can call it like: make run NODE=NodeA
run:
	java -cp target/Distributed_Shared_Memory-1.0-SNAPSHOT.jar NodeMain $(NODE)

# The "clean" target cleans the project
clean:
	mvn clean
