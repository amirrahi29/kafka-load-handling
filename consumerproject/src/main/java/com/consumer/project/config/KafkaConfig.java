package com.consumer.project.config;

import com.consumer.project.model.Patient;
import com.consumer.project.repository.PatientRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class KafkaConfig {

    @Autowired
    private PatientRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    private final List<Patient> buffer = Collections.synchronizedList(new ArrayList<>());
    private static final int BATCH_SIZE = 1000;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    @KafkaListener(topics = AppConstants.PATIENT_UPDATE_TOPIC, groupId = AppConstants.PATIENT_UPDATE_GROUP_ID)
    public void updatedAnyName(@Payload String message,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        try {
            String[] parts = message.split(",");

            if (parts.length < 2 || parts[0].isBlank() || parts[1].isBlank()) {
                System.err.println("Skipped invalid message: " + message);
                return;
            }

            String name = parts[0].trim();
            String roll = parts[1].trim();

            Patient patient = new Patient();
            patient.setName(name);
            patient.setRoll(roll);

            buffer.add(patient);
            System.out.println("Partition " + partition + " | Received: " + message);

            if (buffer.size() >= BATCH_SIZE) {
                flushBuffer("Auto Flush (batch size reached)");
            }

        } catch (Exception e) {
            System.err.println("Failed to process message: " + message);
            e.printStackTrace();
        }
    }

    @Scheduled(cron = "0 */1 * * * *") // Every 1 minutes at 0th second
    public void flushBufferEveryFiveMinutes() {
        flushBuffer("Cron Flush (every 5 minutes)");
    }

    @PreDestroy
    public void flushOnShutdown() {
        flushBuffer("Shutdown Flush");
        executor.shutdown();
    }

    private void flushBuffer(String reason) {
        if (!buffer.isEmpty()) {
            List<Patient> batch;
            synchronized (buffer) {
                batch = new ArrayList<>(buffer);
                buffer.clear();
            }

            executor.submit(() -> {
                try {
                    repository.saveAll(batch);
                    System.out.println(reason + ": Saved " + batch.size() + " records");
                } catch (Exception e) {
                    System.err.println(reason + ": Error saving batch");
                    e.printStackTrace();
                }
            });
        }
    }
}
