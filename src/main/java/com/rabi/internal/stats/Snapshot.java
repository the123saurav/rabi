package com.rabi.internal.stats;

public class Snapshot<T> extends Base {

    private T val;

    Snapshot(String name) {
        super(name);
    }

    public void set(T v) {
        val = v;
    }

    public T get() {
        return val;
    }
}
