package com.amazon.dw.grasshopper.attributesearchservice;


/**
 * Exceptions for GHQuery Construction Module.
 * @author mounicam
 */
public class GrassHopperQueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public GrassHopperQueryException(String message) {
        super(message);
    }

    public GrassHopperQueryException(String message, Throwable cause) {
        super(message, cause);
    }

}

