package com.mediator.s3gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Application entry point for the standalone S3-compatible nearline gateway.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class S3GatewayApplication {

    /**
     * Starts Spring Boot, creates the configured services and opens the HTTP(S)
     * server defined in application.yml.
     */
    public static void main(String[] args) {
        SpringApplication.run(S3GatewayApplication.class, args);
    }
}
