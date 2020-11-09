package com.rabi.internal.db.engine;

import com.rabi.internal.types.ByteArrayWrapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public interface Index {

  long getId();

  Path getPath();

  void setPath(Path p);

  byte[] getMinKey();

  long getMinKeyOffset();

  long getMaxKeyOffset();

  void setMinKeyInfo(byte[] k, long offset);

  byte[] getMaxKey();

  void setMaxKeyInfo(byte[] k, long offset);

  long getTotalKeys();

  void setTotalKeys(long t);

  void put(byte[] key, long offset);

  long get(byte[] k);

  void flush(boolean syncMode) throws IOException;

  void rename(Path n) throws IOException;

  void lockAndSignal();

  void unlockAndSignal();

  double getDensity();

  List<ByteArrayWrapper> getKeys();

  void unlink();
}
