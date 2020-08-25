package com.rabi.internal.db.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Data file interface.
 */
public interface Data {

  void flush(List<Pair<byte[], byte[]>> e) throws IOException;

  void rename(Path n) throws IOException;
}
