package com.rabi.internal.db.engine.channel.message;

import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.channel.Message;
import java.nio.file.Path;

public class EngineToFlusher extends Message {
  private final MemTable m;
  private final boolean syncMode;
  private final Path dataDir;

  public EngineToFlusher(MemTable m, boolean sm, Path d) {
    this.m = m;
    this.syncMode = sm;
    this.dataDir = d;
  }

  public MemTable getMemTable() {
    return m;
  }

  public boolean getSyncMode() {
    return syncMode;
  }

  public Path getDataDir() {
    return dataDir;
  }
}
