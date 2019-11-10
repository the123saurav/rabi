package com.rabi.exceptions;

public class InvalidDBStateException extends RuntimeException {

    public InvalidDBStateException(String msg) {
        super(msg);
    }
}
