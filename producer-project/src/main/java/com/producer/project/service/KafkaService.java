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
        for (int i = 0; i <= 10000; i++) {
            String message = name + "," + i;
            kafkaTemplate.send(AppConstants.PATIENT_TOPIC_NAME, message);
            logger.info("Message produced: {}", message);
        }        
        return true;
    }

}