package com.rabi;


import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * All operations are idempotent.
 */
public interface DB {
  /**
   * @param cfg Config for rabi, this can be set only once.
   * @return a future whose completion with 'true' indicates DB is ready.
   */
  CompletableFuture<Void> open(Config cfg) throws CloneNotSupportedException;

  /**
   * @param k key to get value for.
   * @return value if exists else null
   * throws InvalidDBStateException if DB is not running.
   */
  byte[] get(byte[] k);

  /**
   * puts a key.
   *
   * @param k key should be less than 256 bytes
   * @param v value should be less than 65535(2^16 - 1) B.
   * @throws IOException
   */
  void put(byte[] k, byte[] v) throws IOException;

  /**
   * deletes a key.
   *
   * @param k key
   * @return whether the operation succeeded.
   * throws InvalidDBStateException if DB is not running.
   * This doesn't err if key doesn't exist.
   */
  void delete(byte[] k);

  /**
   * stops the DB. This doesn't block caller but returns Future which caller
   * can wait on for clean exit.
   *
   * @return future wrapping status indicating success.
   * @throws RuntimeException
   */
  CompletableFuture<Void> stop();
}
