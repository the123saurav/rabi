package com.rabi.internal.db.engine;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Data file interface.
 */
public interface Data {

  long getID();

  Path getPath();

  long getSize();

  void loadValues() throws IOException;

  void offloadValues();

  byte[] getValueForOffset(long offset);

  void flush(List<Pair<byte[], byte[]>> e, boolean syncMode) throws IOException;

  void rename(Path n) throws IOException;

  void unlink();
}
