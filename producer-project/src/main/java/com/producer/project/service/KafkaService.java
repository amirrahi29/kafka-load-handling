package com.producer.project.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.producer.project.config.AppConstants;
import com.producer.project.util.CsvLoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

@Service
public class KafkaService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Logger logger = LoggerFactory.getLogger(KafkaService.class);

    // Dummy data send with a given name
    public boolean updatePatient(String name) {
        int numPartitions = 16;
        for (int i = 0; i <= 100000; i++) {
            String message = name + "," + i;
            int partition = i % numPartitions;
            kafkaTemplate.send(AppConstants.PATIENT_TOPIC_NAME, partition, null, message);
            logger.info("Sent to partition {}: {}", partition, message);

            if (i % 500 == 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return true;
    }

    // CSV Data Send
    public boolean updatePatientFromCsv() {
        int numPartitions = 16;
        String filePath = "src/main/resources/patient.csv";
        int count = 0;
        int skipped = 0;

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] line;
            reader.readNext();

            while ((line = reader.readNext()) != null) {
                //  Validate row: must have 2 non-blank fields
                if (line.length < 2 || line[0] == null || line[1] == null || line[0].isBlank() || line[1].isBlank()) {
                    CsvLoggerUtil.logInvalidCsvRow(line);
                    logger.warn("Skipping invalid CSV row: {}", Arrays.toString(line));
                    skipped++;
                    continue;
                }

                String name = line[0].trim();
                String roll = line[1].trim();

                String message = name + "," + roll;
                int partition = count % numPartitions;

                kafkaTemplate.send(AppConstants.PATIENT_TOPIC_NAME, partition, null, message);
                logger.info("Sent to partition {}: {}", partition, message);

                count++;

                if (count % 500 == 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            logger.info("Total valid messages sent from CSV: {}", count);
            logger.info("⚠Total invalid/skipped rows: {}", skipped);

        } catch (IOException | CsvValidationException e) {
            logger.error("Error reading CSV file", e);
            return false;
        }

        return true;
    }
}
