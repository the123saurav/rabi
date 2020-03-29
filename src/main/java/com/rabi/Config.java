package com.rabi;

import com.rabi.exceptions.BadConfigException;

/**
 * Config passed to open() for DB.
 */
public final class Config implements Cloneable {

    public enum ShutdownMode {
        GRACEFUL, FAST
    }

    private static final int maxMemtables = 4;

    private static final int maxFlushedFiles = 4;

    private static final int minOrphanedKeysDuringCompaction = 100;

    private static final int maxL3FileSizeMB = 1000;

    private final long memtableMaxKeys;

    private final short memtableSegments;

    private final short bootParallelism;

    private final boolean sync;

    private final ShutdownMode shutdownMode;

    private Config(long memMax, short parallelism, short memtableSegments, boolean sync, ShutdownMode shutdownMode) {
        this.memtableMaxKeys = memMax;
        this.bootParallelism = parallelism;
        this.memtableSegments = memtableSegments;
        this.sync = sync;
        this.shutdownMode = shutdownMode;
    }

    public short getBootParallelism() {
        return bootParallelism;
    }

    public int getMaxMemtables() {
        return maxMemtables;
    }

    public long getMemtableMaxKeys() {
        return memtableMaxKeys;
    }

    public int getMaxFlushedFiles() {
        return maxFlushedFiles;
    }

    public short getMemtableSegments() {
        return memtableSegments;
    }

    public boolean getSync() {
        return sync;
    }

    public ShutdownMode getShutdownMode() {
        return shutdownMode;
    }

    @Override
    public String toString() {
        return String.format("Config{%nmemtableMaxKeys: %d,%n"
                        + "maxMemtables: %d,%n"
                        + "maxFlushedFiles: %d,%n"
                        + "minOrphanedKeysDuringCompaction: %d,%n"
                        + "bootParallelism: %s,%n"
                        + "memtableSegments: %s,%n"
                        + "maxL3FileSizeMB: %d%n}",
                memtableMaxKeys, maxMemtables, maxFlushedFiles,
                minOrphanedKeysDuringCompaction, bootParallelism, memtableSegments,
                maxL3FileSizeMB);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public static final class ConfigBuilder {

        private static final long DEFAULT_MEMTABLE_MAX_KEYS = 100000;
        private static final short DEFAULT_BOOT_PARALLELISM = 1;
        private static final int MAX_MEMTABLE_SEGMENTS = 16;
        private static final int DEFAULT_MEMTABLE_SEGMENTS = 4;
        private static final int MEMTABLE_MINIMUM_MAX_KEYS = 100000;
        private static final boolean DEFAULT_FILE_SYNC = false;
        private static final ShutdownMode DEFAULT_SHUTDOWN_MODE = ShutdownMode.GRACEFUL;

        private static final String MEMTABLE_MAX_KEYS_ERR_STRING = "memtableMaxKeys should be atleast " + MEMTABLE_MINIMUM_MAX_KEYS;
        private static final String MEMTABLE_MAX_SEGMENTS_ERR_STRING = "memtableMaxSegments should be less than " + MAX_MEMTABLE_SEGMENTS;

        private long memtableMaxKeys = DEFAULT_MEMTABLE_MAX_KEYS;
        private short memtableSegments = DEFAULT_MEMTABLE_SEGMENTS;
        private short bootParallelism = DEFAULT_BOOT_PARALLELISM;
        private boolean sync = DEFAULT_FILE_SYNC;
        private ShutdownMode shutdownMode = DEFAULT_SHUTDOWN_MODE;

        public ConfigBuilder setMemtableMaxKeys(long m) {
            memtableMaxKeys = m;
            return this;
        }

        public ConfigBuilder setBootParallelism(short m) {
            bootParallelism = m;
            return this;
        }

        //note that this impacts only new memtables and not existing ones.
        public ConfigBuilder setMemtableSegments(short m) {
            memtableSegments = m;
            return this;
        }

        public ConfigBuilder setSync(boolean s) {
            sync = s;
            return this;
        }

        public ConfigBuilder setShutdownMode(ShutdownMode s) {
            shutdownMode = s;
            return this;
        }

        public Config build() {
            if (memtableMaxKeys < MEMTABLE_MINIMUM_MAX_KEYS) {
                throw new BadConfigException(MEMTABLE_MAX_KEYS_ERR_STRING);
            }
            if (memtableSegments > MAX_MEMTABLE_SEGMENTS) {
                throw new BadConfigException(MEMTABLE_MAX_SEGMENTS_ERR_STRING);
            }
            return new Config(memtableMaxKeys, bootParallelism, memtableSegments, sync, shutdownMode);
        }
    }
}
