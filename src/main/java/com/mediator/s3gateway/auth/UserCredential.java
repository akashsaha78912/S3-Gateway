package com.mediator.s3gateway.auth;

import java.time.Instant;

/**
 * One JSON-backed gateway user and their assigned access key.
 *
 * The access key identifies the user. It is generated once during user
 * creation and remains the same until the credential is replaced.
 */
public record UserCredential(
        String userId,
        String username,
        String accessKeyId,
        boolean enabled,
        Instant createdAt
) {
}