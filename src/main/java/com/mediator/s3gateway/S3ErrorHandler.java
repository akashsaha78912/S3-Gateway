package com.mediator.s3gateway;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.servlet.http.HttpServletRequest;

@RestControllerAdvice
class S3ErrorHandler {

    @ExceptionHandler(S3Exception.class)
    ResponseEntity<String> s3(S3Exception e, HttpServletRequest request) {
        return xml(e.status(), e.code(), e.getMessage(), e.resource(), request);
    }

    @ExceptionHandler(Exception.class)

    ResponseEntity<String> unexpected(Exception e, HttpServletRequest request) {
        return xml(500, "InternalError", "We encountered an internal error. Please try again.", request.getRequestURI(), request);
    }

    private ResponseEntity<String> xml(int status, String code, String message, String resource, HttpServletRequest request) {
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + esc(code) + "</Code><Message>" + esc(message) + "</Message><Resource>" + esc(resource) + "</Resource><RequestId>" + attribute(request, "requestId") + "</RequestId><HostId>" + attribute(request, "extendedRequestId") + "</HostId></Error>";
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_XML).body(body);
    }

    private String attribute(HttpServletRequest request, String name) {
        return esc(request.getAttribute(name) == null ? "" : request.getAttribute(name).toString());
    }

    private String esc(String value) {
        return value == null ? "" : value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
