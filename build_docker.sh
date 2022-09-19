#!/bin/sh
./gradlew fatJar
docker build -t duckdb-test:v1.0 -f docker/Dockerfile .
