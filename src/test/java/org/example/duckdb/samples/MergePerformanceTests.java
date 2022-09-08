package org.example.duckdb.samples;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.duckdb.common.ResultSetUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.joining;
import static org.example.duckdb.common.MemoryUtils.triggerGc;
import static org.example.duckdb.common.MemoryUtils.usedMemoryInMb;

@Slf4j
public class MergePerformanceTests {
    private static final String TABLE_NAME = "test_memory_limit";
    private static final String PATH_TO_RESOURCES = "src/test/resources";
    private static final ThreadLocal<Integer> count = ThreadLocal.withInitial(() -> 0);

    @Test
    public void driveMerge() {
        triggerGc();
        log.debug("Used memory at the beginning: {} MB", usedMemoryInMb());

        String incomingDataPath = PATH_TO_RESOURCES + "/incoming_data";
        String existingDataPath = PATH_TO_RESOURCES + "/existing_files";
        executeQueriesUsingTable(new File(existingDataPath), new File(incomingDataPath), TABLE_NAME, 2);
    }

    @SneakyThrows
    public void executeQueriesUsingTable(@NonNull File existingDir, @NonNull File incomingDataDir, @NonNull String table,
                                         int countOfExistingFilesToRewrite) {

        File rewrittenFilesDir = createTempDirForRewrites(table);

        try (Connection conn = createDuckDbConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Taking PKs from the incoming parquet file and creating table with that
                String incomingPksTableCreationStmt =
                        "CREATE TABLE test_incoming_table AS SELECT {delete_keys} FROM read_parquet('{incoming_files}')"
                                .replace("{delete_keys}", createDeleteClause(List.of("id")))
                                .replace("{incoming_files}", incomingDataDir.getPath() + "/*.parquet");

                log.debug("Executing statement: {}", incomingPksTableCreationStmt);
                stmt.execute(incomingPksTableCreationStmt);
                log.info("Used memory after creating table for incoming files: {} MB", usedMemoryInMb());

                int count = 0;
                for (File existingFile : existingDir.listFiles()) {
                    // Creating a table for the existing file
                    String existingFileTableCreationStmt =
                            "CREATE TABLE test_existing_table AS SELECT * FROM read_parquet('{existing_file}')"
                                    .replace("{existing_file}", existingFile.getPath());

                    log.info("Executing query: {}", existingFileTableCreationStmt);
                    stmt.execute(existingFileTableCreationStmt);

                    String rewriteFilePath = rewrittenFilesDir.getPath() + "/" + generateUniqueParquetFileName();

                    String mergeStmt =
                            "COPY (SELECT {column_clause} FROM test_existing_table WHERE {delete_keys} NOT IN "
                                    + "(SELECT {delete_keys} FROM test_incoming_table))"
                                    + "TO '{new_parquet_file}' (FORMAT 'parquet')";
                    mergeStmt =
                            mergeStmt.replace("{column_clause}", "*")
                                    .replace("{delete_keys}", createDeleteClause(List.of("id")))
                                    .replace("{new_parquet_file}", rewriteFilePath);

                    log.info("Executing query: {}", mergeStmt);
                    stmt.execute(mergeStmt);

                    long recordCount = extractRecordCount(rewriteFilePath, stmt);
                    log.info("Affected {} records", recordCount);

                    // Cleaning up temporary table
                    String dropTableQuery = "DROP TABLE test_existing_table";
                    log.info("Executing query:" + dropTableQuery);
                    stmt.execute(dropTableQuery);

                    if (countOfExistingFilesToRewrite > 0 && count++ >= countOfExistingFilesToRewrite) {
                        break;
                    }
                }
            }
        }
        triggerGc();
        log.debug("Used memory at the end: {} MB", usedMemoryInMb());
    }

    private File createTempDirForRewrites(String table) {
        File dedupedDataDir = createTempDir(table);
        log.info("dedupedDataDir path: {}", dedupedDataDir.getPath());
        dedupedDataDir.deleteOnExit();
        return dedupedDataDir;
    }

    @SneakyThrows
    @Test
    public void columnMetadataInIncomingFile() {
        @Cleanup
        var connection = DriverManager.getConnection("jdbc:duckdb:");

        @Cleanup
        var statement = connection.createStatement();

        String sql = "SELECT * FROM parquet_metadata('" + PATH_TO_RESOURCES +
                "/incoming_data/1.parquet" + "')";

        @Cleanup
        ResultSet resultSet = statement.executeQuery(sql);
        log.info("Column Name | Type");
        log.info("--------------------------");

        while (resultSet.next()) {
            log.info("{} | {}", resultSet.getString("path_in_schema"),
                    resultSet.getString("type"));
        }
    }

    @SneakyThrows
    @Test
    public void columnDataInIncomingFile() {
        @Cleanup
        var conn = DriverManager.getConnection("jdbc:duckdb:");

        @Cleanup
        var statement = conn.createStatement();

        var sql = "SELECT * FROM '" + PATH_TO_RESOURCES +  "/incoming_data/1.parquet" + "'";

        @Cleanup
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetUtils.printData(resultSet, 2);
    }

    @SneakyThrows
    private static File createTempDir(String table) {
        String prefix = table.replace(".", "_") + "_" + UUID.randomUUID().toString().replace("-", "_") + "_";
        return Files.createTempDirectory(prefix).toFile();
    }

    public static String generateUniqueParquetFileName() {
        count.set(count.get() + 1);
        return Instant.now().toString().replace(":", "-") + "_" + count.get() + ".parquet";
    }

    private static Connection createDuckDbConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        @Cleanup
        Statement stmt = conn.createStatement();
        stmt.execute("PRAGMA memory_limit='1GB'");
        stmt.execute("PRAGMA threads=10");
        stmt.execute(String.format("PRAGMA temp_directory='%s'", "/data/kamal_test"));

        return conn;
    }

    private static long extractRecordCount(String file, Statement stmt) throws SQLException {
        String countQuery = String.format("SELECT COUNT(1) FROM read_parquet('%s')", file);
        try (ResultSet rs = stmt.executeQuery(countQuery)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String createDeleteClause(List<String> deleteColumns) {
        if (deleteColumns.isEmpty()) return "";
        String deleteKeyClause = deleteColumns.stream().map(c -> quote(c)).collect(joining(", "));
        return deleteColumns.size() > 1 ? "(" + deleteKeyClause + ")" : deleteKeyClause;
    }

    private static String quote(String symbol) {
        return '"' + symbol.replace("\"", "\"\"") + '"';
    }
}