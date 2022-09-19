package org.example.duckdb;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * Simulates merges of incoming records from Core into existing files in S3.
 *
 * Note: Some of the code present here is a faithful reproduction of unoptimized code used elsewhere.
 */
@Slf4j
public class RecordsProcessor {

    private static final ThreadLocal<Integer> fileSuffix = ThreadLocal.withInitial(() -> 0);

    private final List<String> connectionConfig;

    public RecordsProcessor (@NonNull List<String> connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    // Tries to mimic existing code elsewhere
    @SneakyThrows
    public void mergeUsingIntermediateTables(@NonNull File existingDir, @NonNull File incomingDataDir, @NonNull String table,
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
                    log.info("Processing existing file {}", existingFile.getPath());

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

    // Tries to mimic existing code elsewhere
    @SneakyThrows
    public void mergeWithoutTables(File existingDir, File incomingDataDir, @NonNull String table) {
        File rewrittenFilesDir = createTempDir(table);
        rewrittenFilesDir.deleteOnExit();

        try (Connection conn = createDuckDbConnection()) {
            int iteration = 0;
            int maxIterationCount = 15;
            while (iteration++ <= maxIterationCount) {
                try (Statement stmt = conn.createStatement()) {
                    for (File existingFile : existingDir.listFiles()) {
                        log.debug("Processing existing file -- {}", existingFile.getPath());

                        String rewriteFilePath = rewrittenFilesDir.getPath() + "/" + generateUniqueParquetFileName();
                        String mergeStmt =
                                "COPY (SELECT {column_clause} FROM read_parquet('{existing_parquet}') WHERE {delete_keys} NOT IN "
                                        + "(SELECT {delete_keys} FROM read_parquet('{incoming_parquets}'))) "
                                        + "TO '{new_parquet_file}' (FORMAT 'parquet')";

                        String columnClause = "\"id\", \"long_col_0\", \"long_col_1\", \"instant_col_1\", \"instant_col_2\", " +
                                "\"instant_col_3\", \"double_col_0\", \"double_col_2\",  \"double_col_1\", \"instant_col_0\", " +
                                "\"double_col_3\", \"string_col_0\", \"string_col_3\", \"string_col_1\", \"long_col_2\", " +
                                "\"string_col_2\", \"long_col_3\", \"_fivetran_synced\"";

                        mergeStmt = mergeStmt.replace("{column_clause}", columnClause);
                        mergeStmt = mergeStmt.replace("{delete_keys}", createDeleteClause(List.of("id")));
                        mergeStmt = mergeStmt.replace("{incoming_parquets}", incomingDataDir.getPath() + "/*.parquet");
                        mergeStmt = mergeStmt.replace("{existing_parquet}", existingFile.getPath());
                        mergeStmt = mergeStmt.replace("{new_parquet_file}", rewriteFilePath);
                        log.info("Executing query: {}", mergeStmt);

                        (new File(rewriteFilePath)).delete();

                        stmt.execute(mergeStmt);

                        long recordCount = extractRecordCount(rewriteFilePath, stmt);
                        log.info("Affected {} records", recordCount);
                    }
                }
            }
        }
    }

    private File createTempDirForRewrites(String name) {
        File dir = createTempDir(name);
        dir.deleteOnExit();
        return dir;
    }

    @SneakyThrows
    private static File createTempDir(String table) {
        String prefix = table.replace(".", "_") + "_" + UUID.randomUUID().toString().replace("-", "_") + "_";
        return Files.createTempDirectory(prefix).toFile();
    }

    public static String generateUniqueParquetFileName() {
        fileSuffix.set(fileSuffix.get() + 1);
        return Instant.now().toString().replace(":", "-") + "_" + fileSuffix.get() + ".parquet";
    }

    @SneakyThrows
    private Connection createDuckDbConnection() {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");

        File tempDataDir = Files.createTempDirectory("duckdb-temp-files").toFile();
        tempDataDir.deleteOnExit();

        String tempDataDirPath = tempDataDir.getAbsolutePath();

        log.info("tempDir: {}", tempDataDirPath);

        try (Statement stmt = conn.createStatement()) {
            for (String connectionConfigStmt : connectionConfig) {
                stmt.execute(connectionConfigStmt);
            }
            stmt.execute(String.format("PRAGMA temp_directory='%s'", tempDataDirPath));
        }
        return conn;
    }

    private long extractRecordCount(String file, Statement stmt) throws SQLException {
        String countQuery = String.format("SELECT COUNT(1) FROM read_parquet('%s')", file);
        try (ResultSet rs = stmt.executeQuery(countQuery)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private String createDeleteClause(List<String> deleteColumns) {
        if (deleteColumns.isEmpty()) return "";
        String deleteKeyClause = deleteColumns.stream().map(c -> quote(c)).collect(joining(", "));
        return deleteColumns.size() > 1 ? "(" + deleteKeyClause + ")" : deleteKeyClause;
    }

    private String quote(String symbol) {
        return '"' + symbol.replace("\"", "\"\"") + '"';
    }
}
