package com.rabi.internal.db;

import com.rabi.Config;
import com.rabi.DB;
import com.rabi.exceptions.DBUinitializedException;
import com.rabi.exceptions.ShutdownException;
import com.rabi.internal.db.engine.EngineImpl;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

/**
 * This class is a Facade over DB engine which is an FSM.
 * The Stats class is also part of DB engine(should that be part of DBImpl and updates are sent to
 * it via queue just to fasten it up, maybe in future). DBEngine would have
 * state and it will reject calls to it unless running.
 */

public final class DBImpl implements DB {

  private final String dataDir;
  private final Stats stats;
  private final Logger log; //pass NOP logger to turn off logging

  private Engine engine;

  private static final DBUinitializedException uninitializedError = new DBUinitializedException();

  private static final Map<String, DBImpl> instances = new HashMap<>();

  /**
   * implementation of db.
   *
   * @param dataDir data directory
   * @param logger  logger
   */

  public static synchronized DB get(final String dataDir, final Logger logger) {
    DBImpl instance = instances.get(dataDir);
    if (instance == null) {
      instance = new DBImpl(dataDir, logger);
      instances.put(dataDir, instance);
    }
    return instance;
  }

  private DBImpl(String dDir, Logger logger) {
    this.dataDir = dDir;
    this.log = logger;
    this.stats = new Stats(logger);
  }

  /**
   * open sets up db.
   * Synchronised
   *
   * @param c Config for rabi, this can be set only once.
   * @return a future whose completion indicates DB is ready for query.
   */
  @Override
  public synchronized CompletableFuture<Void> open(Config c) throws CloneNotSupportedException {
    if (engine != null) {
      return CompletableFuture.completedFuture(null);
    }
    final Config cfg = (Config) c.clone();
    engine = new EngineImpl(this.dataDir, cfg, this.log);
    return CompletableFuture.runAsync(engine::start);
  }

  @Override
  public byte[] get(byte[] k) {
    if (engine == null) {
      throw uninitializedError;
    }
    //note that engine might not have initialised here, but we let engine handle that.
    //call engine here
    return new byte[0];
  }

  @Override
  public void put(byte[] k, byte[] v) throws IOException {
    if (engine == null) {
      throw uninitializedError;
    }
    engine.put(k, v);
  }

  @Override
  public void delete(byte[] k) {
    if (engine == null) {
      throw uninitializedError;
    }
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    if (engine == null) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.runAsync(() -> {
      try {
        engine.shutdown();
        instances.remove(dataDir);
      } catch (final Exception e) {
        throw new ShutdownException(e);
      }
    });
  }
}
