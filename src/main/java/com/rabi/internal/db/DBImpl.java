package com.rabi.internal.db;

import com.rabi.Config;
import com.rabi.DB;
import com.rabi.exceptions.DBUinitializedException;
import com.rabi.internal.db.engine.EngineImpl;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * This class is still a Facade over DB engine which is an FSM.
 * The Stats class is also part of DB engine(should that be part of DBImpl and updates are sent to
 * it via queue just to fasten it up, maybe in future). DBEngine would have
 * state and it will reject calls to it unless running.
 * <p>
 */

//TODO use import com.google.common.base.Preconditions;
//this class should not be exposed outside of Factory.
public final class DBImpl implements DB {

    private final String dataDir;
    private final Stats stats;
    private final Logger log; //pass NOP logger to turn off logging

    private Engine engine;

    private final DBUinitializedException uninitializedError = new DBUinitializedException();

    /**
     * implementation of db.
     *
     * @param dDir   data directory
     * @param logger logger
     */
    public DBImpl(String dDir, Logger logger) {
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
    public CompletableFuture<Void> open(Config c) throws InterruptedException {
        Config cfg = null;
        try {
            cfg = (Config) c.clone();
        } catch (CloneNotSupportedException e) {
        }
        if (engine != null) {
            return CompletableFuture.completedFuture(null);
        }
        synchronized (DBImpl.class) {
            if (engine != null) {
                return CompletableFuture.completedFuture(null);
            }
            engine = new EngineImpl(this.dataDir, cfg, this.log);
            return CompletableFuture.runAsync(engine::start);
        }
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
    public void  put(byte[] k, byte[] v) throws IOException {
        if (engine == null) {
            throw uninitializedError;
        }
        engine.put(k, v);
    }

    @Override
    public boolean delete(byte[] k) {
        if (engine == null) {
            throw uninitializedError;
        }
        return false;
    }

    @Override
    public CompletableFuture<Boolean> stop() {
        if (engine == null) {
            return CompletableFuture.completedFuture(true);
        }
        //delegate to engine
        return null;
    }
}
