package com.mjdsoftware.kafkatool;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * I am the main entry point for this sping boot application
 */
@SpringBootApplication
public class KafkaToolApplication {

    /**
     * Answer an instance for the following arguments
     */
    public KafkaToolApplication() {
        super();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        SpringApplication.run(KafkaToolApplication.class, args);

    }
}
