package com.amazon.dw.grasshopper.attributesearchservice;

public class CloudSearchException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CloudSearchException(String message) {
        super(message);
    }

    public CloudSearchException(String message, Throwable cause) {
        super(message, cause);
    }

}

