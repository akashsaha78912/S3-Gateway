package com.mediator.s3gateway.web;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Adds the two request identifiers expected on S3 responses.
 *
 * <p>The same values are stored as request attributes so S3ErrorHandler can
 * include them inside XML error documents.
 */
@Component
class RequestIdFilter extends OncePerRequestFilter {

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Runs once for every request before it reaches S3Controller.
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        // The short request ID is UUID-based and rendered without hyphens.
        String id = UUID.randomUUID().toString().replace("-", "");

        // The extended request ID contains 256 bits of random data.
        byte[] extendedBytes = new byte[32];
        RANDOM.nextBytes(extendedBytes);
        String extendedId = Base64.getEncoder().encodeToString(extendedBytes);
        request.setAttribute("requestId", id);
        request.setAttribute("extendedRequestId", extendedId);
        response.setHeader("x-amz-request-id", id);
        response.setHeader("x-amz-id-2", extendedId);
        // Continue into Spring routing after both IDs are available.
        chain.doFilter(request, response);
    }
}
