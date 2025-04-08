# Makefile
.PHONY: all compile clean

# Default target: build (compile) the project
all: compile

# Compile = run Maven's 'clean package', which produces the fat JAR in target/
compile:
	mvn clean package

# Clean = run Maven's 'clean', removing the target/ directory
clean:
	mvn clean
