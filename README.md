# Code for Verifying DuckDB Behavior and Performance

## Building a new fat jar
```
./gradlew fatJar
```

## Building a Docker image
```
./build_docker.sh
```
To remove the Docker image, run: `docker rmi duckdb-test`

## Running a Container and Managing it
```
# Running a container. Add/remove options as needed: -m 12g --memory-reservation=12g --cpus="2".
docker run -d --name duckdb-test-container duckdb-test:v1.0

# Getting inside the container
docker exec -it duckdb-test-container /bin/bash

# Killing and removing the container
docker kill duckdb-test-container
docker rm duckdb-test-container
```

## Gathering Relevant System Resource Stats
```
./capture_stats.sh
```

## Note

1. The project assumes that the test data files (with `.parquet` extension) are available in the `test_data` directory. 
   * `incoming_data_limited` - Incoming data files for the merge. 
   * `existing_files_limited` - Existing files on which the incoming data'll be merged.

2. The merge related driver code in `src/test/java` uses the same folders as above minus the `_limited` suffix. You can
   run those as experiments by executing the test directly from the IDE. 
