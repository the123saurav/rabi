package com.rabi.internal.stats;

public class Counter extends Base {

    private int cnt = 0;

    public Counter(String name) {
        super(name);
    }

    public void increment() {
        cnt++;
    }

    public int value() {
        return cnt;
    }
}
