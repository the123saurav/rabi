package com.rabi;


import org.slf4j.Logger;

import java.util.concurrent.Future;

/**
 * All operations are idempotent.
 */
public interface DB {
    /**
     *
     * @param cfg Config for rabi, this can be set only once.
     * @return a future whose completion with 'true' indicates DB is ready.
     */
    Future<Boolean> open(Config cfg, Logger logger);

    /**
     *
     * @param k : key to get value for.
     * @return value if exists else null
     * throws InvalidDBStateException if DB is not running.
     */
    byte[] get(byte[] k);

    /**
     * puts a key.
     * @param k: key should be less than 256 bytes
     * @param v: value
     * @return whether the operation succeeded.
     *  throws InvalidDBStateException if DB is not running.
     */
    boolean put(byte[] k, byte[] v);

    /**
     * deletes a key.
     * @param k: key
     * @return whether the operation succeeded.
     *  throws InvalidDBStateException if DB is not running.
     *  This doesn't err if key doesn't exist.
     */
    boolean delete(byte[] k);


    /**
     * stops the DB. This doesn't block caller but returns Future which caller
     * can wait on for clean exit.
     * @return future wrapping status indicating success.
     */
    Future<Boolean> stop();





}
