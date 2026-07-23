package com.mediator.s3gateway.exception;

/**
 * Carries an S3-compatible HTTP status, error code, message and resource.
 *
 * <p>S3ErrorHandler converts this exception into the standard XML error body.
 */
public class S3Exception extends RuntimeException {

    private final int status;
    private final String code;
    private final String resource;

    /**
     * Creates an error that can cross the service layer and be rendered as an
     * S3 HTTP response.
     */
    public S3Exception(int status, String code, String message, String resource) {
        super(message);
        this.status = status;
        this.code = code;
        this.resource = resource;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public String resource() {
        return resource;
    }
}
