package com.rabi.exceptions;

public class InitialisationException extends RuntimeException {

  public InitialisationException(Exception e) {
    super(e);
  }

  public InitialisationException(String msg) {
    super(msg);
  }

  public InitialisationException(String msg, Exception e) {
    super(msg, e);
  }
}
