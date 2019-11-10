package com.rabi.internal.db.engine;

public interface Filter extends Loadable {
    boolean mayBePresent(byte[] k);
}
