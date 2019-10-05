package com.rabi;

import com.rabi.exceptions.BadConfigException;

public class Config {

    private static int MEMTABLE_MIN_SIZE_MB = 50;

    private final int maxMemtables = 3;

    private final int maxFlushedFiles = 4;

    private final int minOrphanedKeysDuringCompaction = 100;

    private final int maxL3FileSizeMB = 1000;

    private int memtableMaxSizeMB = 50;


    private Config(int memtableMaxSizeMB){
        this.memtableMaxSizeMB = memtableMaxSizeMB;
    }

    @Override
    public String toString(){
        return String.format("Config{\nmemtableMaxSizeMB: %d,\nmaxMemtables: %d,\n" +
                "maxFlushedFiles: %d,\nminOrphanedKeysDuringCompaction: %d,\nmaxL3FileSizeMB: %d\n}",
                memtableMaxSizeMB, maxMemtables, maxFlushedFiles, minOrphanedKeysDuringCompaction, maxL3FileSizeMB);
    }

    public static class ConfigBuilder{

        private int memtableMaxSizeMB;

        private static final String memTableMaxSizeErrString = "memtableMaxSize should be atleast " + MEMTABLE_MIN_SIZE_MB;

        public ConfigBuilder(){}

        public ConfigBuilder setMemtableMaxSize(int m){
            memtableMaxSizeMB = m;
            return this;
        }

        public Config build(){
            if(memtableMaxSizeMB < MEMTABLE_MIN_SIZE_MB){
                throw new BadConfigException(memTableMaxSizeErrString);
            }
            return new Config(memtableMaxSizeMB);
        }
    }
}
