package com.producer.project.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.producer.project.config.AppConstants;

@Service
public class KafkaService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private Logger logger = LoggerFactory.getLogger(KafkaService.class);

    public boolean updatePatient(String name) {
        int numPartitions = 16;
        for (int i = 0; i <= 100000; i++) {  // ✅ 50 lakh messages
            String message = name + "," + i;
            int partition = i % numPartitions;
            kafkaTemplate.send(AppConstants.PATIENT_TOPIC_NAME, partition, null, message);
            logger.info("Sent to partition {}: {}", partition, message);

            if (i % 500 == 0) {
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
        return true;
    }

}