package org.example.duckdb.samples;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDatabase;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.Statement;

import static org.example.duckdb.common.MemoryUtils.triggerGc;
import static org.example.duckdb.common.MemoryUtils.usedMemoryInMb;

@Slf4j
public class StatementScopeTests {
    private static final int ITERATION_COUNT = 10;
    private static final String TEST_SCHEMA = "myschema";
    private static final String TEST_TABLE = "mytable";
    private static final String TABLE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA;
    private static final String TABLE_CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS " + TEST_SCHEMA + "." + TEST_TABLE + " (\n" +
            "\tid INTEGER NOT NULL,\n" +
            "\tfield_1 VARCHAR,\n" +
            "\tfield_2 VARCHAR,\n" +
            "\tfield_3 VARCHAR,\n" +
            "\tfield_4 VARCHAR,\n" +
            "\tfield_5 VARCHAR,\n" +
            "\tfield_6 VARCHAR,\n" +
            "\tfield_7 VARCHAR,\n" +
            "\tfield_8 VARCHAR,\n" +
            "\tfield_9 VARCHAR,\n" +
            "\tfield_10 VARCHAR,\n" +
            "\tfield_11 VARCHAR,\n" +
            "\tfield_12 VARCHAR,\n" +
            "\tfield_13 VARCHAR,\n" +
            "\tfield_14 VARCHAR,\n" +
            "\tfield_15 VARCHAR,\n" +
            "\tfield_16 VARCHAR,\n" +
            "\tfield_17 VARCHAR,\n" +
            "\tfield_18 VARCHAR,\n" +
            "\tfield_19 VARCHAR,\n" +
            "\tfield_20 VARCHAR,\n" +
            "\tfield_21 VARCHAR,\n" +
            "\tfield_22 VARCHAR,\n" +
            "\tfield_23 VARCHAR,\n" +
            "\tfield_24 VARCHAR,\n" +
            "\tfield_25 VARCHAR,\n" +
            "\tfield_26 VARCHAR,\n" +
            "\tfield_27 VARCHAR,\n" +
            "\tfield_28 VARCHAR,\n" +
            "\tfield_29 VARCHAR,\n" +
            "\tfield_30 VARCHAR,\n" +
            "\tfield_31 VARCHAR,\n" +
            "\tfield_32 VARCHAR,\n" +
            "\tfield_33 VARCHAR,\n" +
            "\tfield_34 VARCHAR,\n" +
            "\tfield_35 VARCHAR,\n" +
            "\tfield_36 VARCHAR,\n" +
            "\tfield_37 VARCHAR,\n" +
            "\tfield_38 VARCHAR,\n" +
            "\tfield_39 VARCHAR,\n" +
            "\tfield_40 VARCHAR,\n" +
            "\tfield_41 VARCHAR,\n" +
            "\tfield_42 VARCHAR,\n" +
            "\tfield_43 VARCHAR,\n" +
            "\tfield_44 VARCHAR,\n" +
            "\tfield_45 VARCHAR,\n" +
            "\tfield_46 VARCHAR,\n" +
            "\tfield_47 VARCHAR,\n" +
            "\tfield_48 VARCHAR,\n" +
            "\tfield_49 VARCHAR,\n" +
            "\tfield_50 VARCHAR\n" +
            ");";

    @SneakyThrows
    @Test
    public void insertsUsingSameStatement() {
        triggerGc();
        log.debug("Used memory - start: {} MB", usedMemoryInMb());

        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=3;");
            statement.execute("PRAGMA memory_limit='0.1GB';");
            statement.execute(TABLE_SCHEMA_SQL);
            statement.execute(TABLE_CREATE_SQL);

            log.debug("Used memory after creating table: {} MB", usedMemoryInMb());

            for (int i = 0; i < ITERATION_COUNT; i++) {
                statement.execute(createInsertStatement(i));
            }
        }
        log.debug("Used memory after inserting rows: {} MB", usedMemoryInMb());

        DuckDBDatabase db = connection.getDatabase();
        connection.close();
        db.shutdown();

        triggerGc();
        log.debug("Used memory at the end: {} MB", usedMemoryInMb());
    }

    @SneakyThrows
    @Test
    public void perInsertStatement() {
        triggerGc();
        log.debug("Used memory - start: {} MB", usedMemoryInMb());

        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=3;");
            statement.execute("PRAGMA memory_limit='0.5GB';");
            statement.execute(TABLE_SCHEMA_SQL);
            statement.execute(TABLE_CREATE_SQL);
        }
        log.debug("Used memory after creating table: {} MB", usedMemoryInMb());

        for (int i = 0; i < ITERATION_COUNT; i++) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(createInsertStatement(i));
            }
        }
        log.debug("Used memory after inserting rows: {} MB", usedMemoryInMb());

        DuckDBDatabase db = connection.getDatabase();
        connection.close();
        db.shutdown();

        triggerGc();
        log.debug("Used memory at the end: {} MB", usedMemoryInMb());
    }

    private String createInsertStatement(int i) {
        StringBuilder queryBuilder = new StringBuilder(
                String.format("INSERT INTO %s.%s VALUES (", TEST_SCHEMA, TEST_TABLE));
        queryBuilder.append(i + ", ");
        for (int j = 1; j < 50; j++) {
            queryBuilder.append("'column-" + j + "', ");
        }
        queryBuilder.append("'column-50')");
        return queryBuilder.toString();
    }
}
