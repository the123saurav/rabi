package com.rabi.internal.db.engine;

import java.io.IOException;
import java.nio.file.Path;

public interface Index {

  long getId();

  Path getPath();

  void put(byte[] key, long offset);

  long get(byte[] k);

  void flush(boolean syncMode) throws IOException;

  void rename(Path n) throws IOException;

  void lockAndSignal();

  void unlockAndSignal();

  double getDensity();
}
