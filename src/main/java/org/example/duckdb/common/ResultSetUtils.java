package org.example.duckdb.common;

import lombok.NonNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetUtils {

    public static void printData(@NonNull ResultSet rs) throws SQLException {
        printData(rs, Integer.MAX_VALUE);
    }

    public static void printData(@NonNull ResultSet rs, int rowCount) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();

        // Print the header with column names
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                System.out.print(" | ");
            }
            System.out.print(metadata.getColumnName(i));
        }
        System.out.println("\n---------------------------------------------------------------------");

        // Print data
        int currentRow = 0;
        while (rs.next() && ++currentRow <= rowCount) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    System.out.print(" | ");
                }
                System.out.print(rs.getString(i));
            }
            System.out.println("");
        }
    }

    public static void printMetadata(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int row = 1;
        while (rs.next()) {
            for (int col = 1; col <= md.getColumnCount(); col++) {
                System.out.println(md.getColumnName(col) + "[" + row + "]=" + rs.getString(col) + " ("
                        + md.getColumnTypeName(col) + ")");
            }
            row++;
        }
    }
}
