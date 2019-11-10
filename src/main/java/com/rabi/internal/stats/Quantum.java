package com.rabi.internal.stats;

public class Quantum extends Base {

    private Number val;
    private final int quanta;

    Quantum(String name, int q) {
        super(name);
        quanta = q;
    }

    //TODO implement a moving window metric


}
