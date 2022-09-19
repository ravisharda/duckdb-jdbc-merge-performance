package org.example.duckdb;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Arrays;

import static org.example.duckdb.common.MemoryUtils.triggerGc;
import static org.example.duckdb.common.MemoryUtils.usedMemoryInMb;

@Slf4j
public class Main {
    private static final String TABLE_NAME = "test_memory_limit";
    private static final String PATH_TO_TEST_DATA = "/opt/duckdb-jdbc/data/";

    RecordsProcessor processor = new RecordsProcessor(Arrays.asList("PRAGMA memory_limit='5GB'", "PRAGMA threads=10"));

    @SneakyThrows
    public static void main(String[] args) {
        Main main = new Main();
        main.driveMergeWithoutTables();
    }

    public void driveMergeWithoutTables() {
        triggerGc();
        log.debug("Used memory at the beginning: {} MB", usedMemoryInMb());

        String incomingDataPath = PATH_TO_TEST_DATA + "/incoming_files";
        String existingDataPath = PATH_TO_TEST_DATA + "/existing_files";
        processor.mergeWithoutTables(new File(existingDataPath), new File(incomingDataPath), TABLE_NAME, 40);
    }
}