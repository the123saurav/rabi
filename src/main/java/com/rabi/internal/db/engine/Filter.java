package com.rabi.internal.db.engine;

public interface Filter extends Bootable<Void> {
    boolean mayBePresent(byte[] k);
}
