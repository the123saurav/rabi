package com.rabi.exceptions;

public class ShutdownException extends RuntimeException {
  public ShutdownException(Exception e) {
    super(e);
  }
}
