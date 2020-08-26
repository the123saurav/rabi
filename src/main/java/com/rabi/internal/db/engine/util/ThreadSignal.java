package com.rabi.internal.db.engine.util;

import java.util.concurrent.atomic.AtomicLong;

public class ThreadSignal {
  private final AtomicLong notified;
  private final AtomicLong acked;
  private final Object monitor;

  public ThreadSignal() {
    monitor = new Object();
    notified = new AtomicLong();
    acked = new AtomicLong();
  }

  public void waitFor() throws InterruptedException {
    //spurious wake-up guard
    while (acked.get() == notified.get()) {
      monitor.wait();
    }
    // we will just send 1 signal for all pending ones
    acked.set(notified.longValue() - acked.get());
  }

  public synchronized void signal() {
    monitor.notify();
    notified.incrementAndGet();
  }
}
