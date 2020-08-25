package com.rabi.internal.db;

import java.io.IOException;

public interface Engine {
  enum State {
    INITIALIZED, BOOTING, RUNNING, TERMINATING, TERMINATED
  }

  void start();

  void put(byte[] k, byte[] v) throws IOException;

  void delete(byte[] k) throws IOException;

  void shutdown() throws IOException;
}
