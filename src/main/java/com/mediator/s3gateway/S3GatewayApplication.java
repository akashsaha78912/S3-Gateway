package com.mediator.s3gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class S3GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(S3GatewayApplication.class, args);
    }
}
