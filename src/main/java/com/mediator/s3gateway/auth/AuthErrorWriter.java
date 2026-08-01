package com.mediator.s3gateway.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Writes authentication failures using an S3-compatible XML error body.
 */
@Component
public class AuthErrorWriter {

    public void write(
            HttpServletRequest request,
            HttpServletResponse response,
            int status,
            String code,
            String message
    ) throws IOException {
        String requestId = attribute(request, "requestId");
        String extendedRequestId
                = attribute(request, "extendedRequestId");

        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <Error>
                  <Code>%s</Code>
                  <Message>%s</Message>
                  <Resource>%s</Resource>
                  <RequestId>%s</RequestId>
                  <HostId>%s</HostId>
                </Error>
                """.formatted(
                escape(code),
                escape(message),
                escape(request.getRequestURI()),
                escape(requestId),
                escape(extendedRequestId)
        );

        byte[] body = xml.getBytes(StandardCharsets.UTF_8);

        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_XML_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentLength(body.length);
        response.getOutputStream().write(body);
    }

    private String attribute(
            HttpServletRequest request,
            String name
    ) {
        Object value = request.getAttribute(name);
        return value == null ? "" : value.toString();
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }

        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}
