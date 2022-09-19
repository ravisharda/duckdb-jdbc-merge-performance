FROM gcr.io/fivetran-donkeys/zulu-openjdk:11.0.10

ARG APP_HOME=/opt/duckdb-jdbc

RUN mkdir -p ${APP_HOME}/data/incoming_files \
    && mkdir -p ${APP_HOME}/data/existing_files \
    && mkdir -p ${APP_HOME}/lib/ \
    && mkdir -p ${APP_HOME}/logs/

COPY ./build/libs/duckdb-jdbc-merge-performance-all-1.0-SNAPSHOT.jar ${APP_HOME}/lib/duckdb-jdbc-merge-performance-all-1.0-SNAPSHOT.jar

COPY ./test_data/incoming_files ${APP_HOME}/data/incoming_files
COPY ./test_data/existing_files ${APP_HOME}/data/existing_files

WORKDIR ${APP_HOME}
# ENTRYPOINT ["tail", "-f", "/dev/null"]

ENTRYPOINT ["java","-jar","./lib/duckdb-jdbc-merge-performance-all-1.0-SNAPSHOT.jar"]

