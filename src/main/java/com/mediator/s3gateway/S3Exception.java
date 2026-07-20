package com.mediator.s3gateway;

public class S3Exception extends RuntimeException {

    private final int status;
    private final String code;
    private final String resource;

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
