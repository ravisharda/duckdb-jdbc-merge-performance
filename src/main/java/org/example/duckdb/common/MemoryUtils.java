package org.example.duckdb.common;

public class MemoryUtils {

    public static long usedMemoryInMb() {
        return toMb(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public static void triggerGc() {
        System.gc();
        System.gc();
        System.gc();
    }

    public static long toMb(long bytes) {
        return bytes / (1024L * 1024L);
    }
}
