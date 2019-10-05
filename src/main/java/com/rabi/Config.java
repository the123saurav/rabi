package com.rabi;

import com.rabi.exceptions.BadConfigException;

/**
 * Config passed to open() for DB.
 */
public final class Config {

    private static final int MEMTABLE_MINIMUM_MAX_SIZE_MB = 50;

    private final int maxMemtables = 3;

    private final int maxFlushedFiles = 4;

    private final int minOrphanedKeysDuringCompaction = 100;

    private final int maxL3FileSizeMB = 1000;

    private final int memtableMaxSizeMB;

    private Config(int memMax) {
        this.memtableMaxSizeMB = memMax;
    }

    @Override
    public String toString() {
        return String.format("Config{%nmemtableMaxSizeMB: %d,%n"
                        + "maxMemtables: %d,%n"
                        + "maxFlushedFiles: %d,%n"
                        + "minOrphanedKeysDuringCompaction: %d,%n"
                        + "maxL3FileSizeMB: %d%n}",
                memtableMaxSizeMB, maxMemtables, maxFlushedFiles,
                minOrphanedKeysDuringCompaction, maxL3FileSizeMB);
    }

    public static final class ConfigBuilder {

        private static final String MEMTABLE_MAX_SIZE_ERR_STRING = "memtableMaxSizeMB should be atleast " + MEMTABLE_MINIMUM_MAX_SIZE_MB;
        private static final int DEFAULT_MEMTABLE_MAX_SIZE_MB = 50;

        private int memtableMaxSizeMB = DEFAULT_MEMTABLE_MAX_SIZE_MB;

        public ConfigBuilder() {
        }

        public ConfigBuilder setMemtableMaxSize(int m) {
            memtableMaxSizeMB = m;
            return this;
        }

        public Config build() {
            if (memtableMaxSizeMB < MEMTABLE_MINIMUM_MAX_SIZE_MB) {
                throw new BadConfigException(MEMTABLE_MAX_SIZE_ERR_STRING);
            }
            return new Config(memtableMaxSizeMB);
        }
    }
}
