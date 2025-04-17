package com.producer.project.controller;

import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.producer.project.service.KafkaService;

@RestController
@RequestMapping("/api")
public class PatientController {

    @Autowired
    private KafkaService kafkaService;
    
    @GetMapping("/update_any_name/{name}")
    public ResponseEntity<?> updateName(@PathVariable String name){
        System.out.println("message produce: "+name);
        this.kafkaService.updatePatient(name);
        return new ResponseEntity<>(Map.of("message","pateient updated"),HttpStatus.OK);
    }

    @GetMapping("/update_any_csv")
    public ResponseEntity<?> updateCSV(){
        this.kafkaService.updatePatientFromCsv();
        return new ResponseEntity<>(Map.of("message","pateient updated"),HttpStatus.OK);
    }

}