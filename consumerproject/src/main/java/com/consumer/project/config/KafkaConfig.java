package com.consumer.project.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class KafkaConfig {
    
    @KafkaListener(topics = AppConstants.PATIENT_UPDATE_TOPIC, groupId = AppConstants.PATIENT_UPDATE_GROUP_ID)
    public void updatedAnyName(String value){
        System.out.println("myValue: "+value);
    }

}