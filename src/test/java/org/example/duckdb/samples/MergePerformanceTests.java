package org.example.duckdb.samples;

import lombok.extern.slf4j.Slf4j;
import org.example.duckdb.RecordsProcessor;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.example.duckdb.common.MemoryUtils.triggerGc;
import static org.example.duckdb.common.MemoryUtils.usedMemoryInMb;

@Slf4j
public class MergePerformanceTests {
    private static final String TABLE_NAME = "test_memory_limit";
    private static final String PATH_TO_TESTDATA = "test_data";

    @Test
    public void driveMergeWithoutTables() {
        triggerGc();
        log.debug("Used memory at the beginning: {} MB", usedMemoryInMb());

        String incomingDataPath = PATH_TO_TESTDATA + "/incoming_files";
        String existingDataPath = PATH_TO_TESTDATA + "/existing_files";

        RecordsProcessor processor = new RecordsProcessor(Arrays.asList("PRAGMA memory_limit='3GB'", "PRAGMA threads=10"));
        processor.mergeWithoutTables(new File(existingDataPath), new File(incomingDataPath), TABLE_NAME, 40);
    }
}