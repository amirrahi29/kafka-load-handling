package com.consumer.project;

import java.io.*;
import java.util.Arrays;

class CsvLoggerUtil {

    private static final String INVALID_CSV_PATH = "src/main/resources/invalid_rows.csv";

    public static synchronized void logInvalidCsvRow(String[] row) {
        try (FileWriter fw = new FileWriter(INVALID_CSV_PATH, true);
             BufferedWriter bw = new BufferedWriter(fw)) {
            bw.write(String.join(",", row));
            bw.newLine();
        } catch (IOException e) {
            System.err.println("Failed to log invalid CSV row: " + Arrays.toString(row));
        }
    }
}
