package com.rabi;

import com.rabi.internal.db.DBImpl;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public final class DBFactory {

    private static Map<String, DB> instances = new HashMap<>();

    //hiding the default constructor.
    private DBFactory() {
    }

    /**
     * There can be multiple db instance in an app each identified by a data-dir.
     *
     * @param dataDir - directory for all rabi data
     * @param logger  - logger
     * @return DB instance which needs to be opened.
     * <p>
     * TODO: if DB is closed, how do we clean this UP, call from close()???
     */
    public static synchronized DB getInstance(String dataDir, Logger logger) {
        DB instance = instances.get(dataDir);
        if (instance == null) {
            instance = new DBImpl(dataDir, logger);
            instances.put(dataDir, instance);
        }
        return instance;
    }
}
