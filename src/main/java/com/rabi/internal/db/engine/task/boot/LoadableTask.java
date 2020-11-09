package com.rabi.internal.db.engine.task.boot;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Bootable;
import java.io.IOException;
import org.slf4j.Logger;

import java.util.function.Consumer;

public class LoadableTask<T> extends BaseTask {

  private final Logger log;
  private final Bootable task;
  private Consumer<T> callback;

  public LoadableTask(Bootable<T> t, Logger l) {
    log = l;
    task = t;
  }

  public LoadableTask(Bootable<T> t, Logger l, Consumer<T> c) {
    log = l;
    task = t;
    callback = c;
  }

  @Override
  public void run() {
    log.debug("Running LoadableTask: " + task);
    T t;
    try {
      t = (T) task.boot();
    } catch (final IOException e) {
      throw new InitialisationException(e);
    }
    if (callback != null) {
      callback.accept(t);
    }
  }
}
