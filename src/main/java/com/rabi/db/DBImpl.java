package com.rabi.db;

import com.rabi.Config;
import com.rabi.DB;
import com.rabi.Stats;
import org.slf4j.Logger;

import java.util.concurrent.Future;

public final class DBImpl implements DB {

    private final String dataDir;
    private final Stats stats;
    private final Logger log;

    public DBImpl(String dDir, Logger logger) {
        this.dataDir = dDir;
        this.log = logger;
        this.stats = new Stats(logger);
    }

    @Override
    public Future<Boolean> open(Config cfg) {
        return null;
    }

    @Override
    public byte[] get(byte[] k) {
        return new byte[0];
    }

    @Override
    public boolean put(byte[] k, byte[] v) {
        return false;
    }

    @Override
    public boolean delete(byte[] k) {
        return false;
    }

    @Override
    public Future<Boolean> stop() {
        return null;
    }
}
