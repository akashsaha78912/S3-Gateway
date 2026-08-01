package com.mediator.s3gateway.auth;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the minimal JSON-backed authentication layer.
 */
@ConfigurationProperties(prefix = "gateway.auth")
public record AuthProperties(
        boolean enabled,
        Path usersFile
     // String adminKey
) {

    public AuthProperties {
        if (usersFile == null) {
            usersFile = Path.of(
                    "C:/NLD/.gateway-auth/users.json"
            );
        }
    }
}