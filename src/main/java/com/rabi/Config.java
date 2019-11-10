package com.rabi;

import com.rabi.exceptions.BadConfigException;

/**
 * Config passed to open() for DB.
 */
public final class Config implements Cloneable {

    public enum ShutdownMode {
        GRACEFUL, FAST
    }

    private static final int maxMemtables = 3;

    private static final int maxFlushedFiles = 4;

    private static final int minOrphanedKeysDuringCompaction = 100;

    private static final int maxL3FileSizeMB = 1000;

    private final int memtableMaxSizeMB;

    private final short memtableSegments;

    private final short bootParallelism;

    private final boolean sync;

    private final ShutdownMode shutdownMode;

    private Config(int memMax, short parallelism, short memtableSegments, boolean sync, ShutdownMode shutdownMode) {
        this.memtableMaxSizeMB = memMax;
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
        return String.format("Config{%nmemtableMaxSizeMB: %d,%n"
                        + "maxMemtables: %d,%n"
                        + "maxFlushedFiles: %d,%n"
                        + "minOrphanedKeysDuringCompaction: %d,%n"
                        + "bootParallelism: %s,%n"
                        + "memtableSegments: %s,%n"
                        + "maxL3FileSizeMB: %d%n}",
                memtableMaxSizeMB, maxMemtables, maxFlushedFiles,
                minOrphanedKeysDuringCompaction, bootParallelism, memtableSegments,
                maxL3FileSizeMB);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public static final class ConfigBuilder {

        private static final int DEFAULT_MEMTABLE_MAX_SIZE_MB = 50;
        private static final short DEFAULT_BOOT_PARALLELISM = 1;
        private static final int MAX_MEMTABLE_SEGMENTS = 16;
        private static final int DEFAULT_MEMTABLE_SEGMENTS = 4;
        private static final int MEMTABLE_MINIMUM_MAX_SIZE_MB = 50;
        private static final boolean DEFAULT_FILE_SYNC = false;
        private static final ShutdownMode DEFAULT_SHUTDOWN_MODE = ShutdownMode.GRACEFUL;

        private static final String MEMTABLE_MAX_SIZE_ERR_STRING = "memtableMaxSizeMB should be atleast " + MEMTABLE_MINIMUM_MAX_SIZE_MB;
        private static final String MEMTABLE_MAX_SEGMENTS_ERR_STRING = "memtableMaxSegments should be less than " + MAX_MEMTABLE_SEGMENTS;

        private int memtableMaxSizeMB = DEFAULT_MEMTABLE_MAX_SIZE_MB;
        private short memtableSegments = DEFAULT_MEMTABLE_SEGMENTS;
        private short bootParallelism = DEFAULT_BOOT_PARALLELISM;
        private boolean sync = DEFAULT_FILE_SYNC;
        private ShutdownMode shutdownMode = DEFAULT_SHUTDOWN_MODE;

        public ConfigBuilder setMemtableMaxSize(int m) {
            memtableMaxSizeMB = m;
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
            if (memtableMaxSizeMB < MEMTABLE_MINIMUM_MAX_SIZE_MB) {
                throw new BadConfigException(MEMTABLE_MAX_SIZE_ERR_STRING);
            }
            if (memtableSegments > MAX_MEMTABLE_SEGMENTS) {
                throw new BadConfigException(MEMTABLE_MAX_SEGMENTS_ERR_STRING);
            }
            return new Config(memtableMaxSizeMB, bootParallelism, memtableSegments, sync, shutdownMode);
        }
    }
}
