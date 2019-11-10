package com.rabi.internal.db.engine;

public interface Index extends Loadable {

    void put(byte[] key, long offset);

    long get(byte[] k);
}
