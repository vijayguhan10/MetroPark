package com.example.Metropark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MetroparkApplication {

	public static void main(String[] args) {
		SpringApplication.run(MetroparkApplication.class, args);
	}

}
