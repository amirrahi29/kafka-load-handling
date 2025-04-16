package com.consumer.project;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ConsumerprojectApplication {
	public static void main(String[] args) {
		SpringApplication.run(ConsumerprojectApplication.class, args);
	}
}
