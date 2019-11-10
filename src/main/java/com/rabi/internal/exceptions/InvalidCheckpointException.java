package com.rabi.internal.exceptions;

public class InvalidCheckpointException extends RuntimeException {

    public InvalidCheckpointException(int offset, int lastOffset) {
        super("invalid checkpoint offset: " + offset + ", last offset on WAL: " + lastOffset);
    }
}
