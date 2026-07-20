package com.mediator.s3gateway;

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

@Component
class RequestIdFilter extends OncePerRequestFilter {

    private static final SecureRandom RANDOM = new SecureRandom();

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        String id = UUID.randomUUID().toString().replace("-", "");
        byte[] extendedBytes = new byte[32];
        RANDOM.nextBytes(extendedBytes);
        String extendedId = Base64.getEncoder().encodeToString(extendedBytes);
        request.setAttribute("requestId", id);
        request.setAttribute("extendedRequestId", extendedId);
        response.setHeader("x-amz-request-id", id);
        response.setHeader("x-amz-id-2", extendedId);
        chain.doFilter(request, response);
    }
}
