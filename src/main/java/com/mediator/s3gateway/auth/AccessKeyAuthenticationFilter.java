package com.mediator.s3gateway.auth;

import java.io.IOException;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Authenticates S3 requests using the X-NLD-Access-Key header.
 */
@Component
@Order(2)
public class AccessKeyAuthenticationFilter
        extends OncePerRequestFilter {

    public static final String ACCESS_KEY_HEADER =
            "X-NLD-Access-Key";

    public static final String USER_ID_ATTRIBUTE =
            "authenticatedUserId";

    public static final String USERNAME_ATTRIBUTE =
            "authenticatedUsername";

    private final AuthProperties properties;
    private final JsonUserCredentialStore credentialStore;
    private final AuthErrorWriter errorWriter;

    public AccessKeyAuthenticationFilter(
            AuthProperties properties,
            JsonUserCredentialStore credentialStore,
            AuthErrorWriter errorWriter
    ) {
        this.properties = properties;
        this.credentialStore = credentialStore;
        this.errorWriter = errorWriter;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (!properties.enabled()) {
            return true;
        }

        /*
         * Internal user-management endpoints use the separate admin key.s
         */
        return request.getRequestURI().startsWith("/internal/users");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        String accessKey = request.getHeader(ACCESS_KEY_HEADER);

        if (accessKey == null || accessKey.isBlank()) {
            errorWriter.write(
                    request,
                    response,
                    403,
                    "AccessDenied",
                    "The request is missing an access key."
            );
            return;
        }

        UserCredential user = credentialStore
                .findByAccessKey(accessKey)
                .orElse(null);

        if (user == null) {
            errorWriter.write(
                    request,
                    response,
                    403,
                    "InvalidAccessKeyId",
                    "The access key ID you provided does not exist."
            );
            return;
        }

        if (!user.enabled()) {
            errorWriter.write(
                    request,
                    response,
                    403,
                    "AccessDenied",
                    "The associated user is disabled."
            );
            return;
        }

        request.setAttribute(
                USER_ID_ATTRIBUTE,
                user.userId()
        );

        request.setAttribute(
                USERNAME_ATTRIBUTE,
                user.username()
        );

        filterChain.doFilter(request, response);
    }
}