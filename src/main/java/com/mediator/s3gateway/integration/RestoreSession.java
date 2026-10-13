package com.mediator.s3gateway.integration;

public record RestoreSession(
        String identity,
        int reqID,
        long totalSize,
        boolean created
) {
}