package com.rabi.internal.db.engine;

public interface Filter extends Loadable<Void> {
    boolean mayBePresent(byte[] k);
}
