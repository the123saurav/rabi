package com.rabi.internal.db.engine.task.boot;

import com.rabi.internal.db.engine.Loadable;
import org.slf4j.Logger;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class LoadableTask<T> extends BaseTask {

    private final Logger log;
    private final Loadable task;
    private Consumer<T> callback;

    public LoadableTask(Loadable<T> t, Logger l) {
        log = l;
        task = t;
    }

    public LoadableTask(Loadable<T> t, Logger l, Consumer<T> c) {
        log = l;
        task = t;
        callback = c;
    }

    @Override
    public void run() {
        log.debug("Running LoadableTask: " + task);
        T t = (T)task.load();
        if(callback != null) {
            callback.accept(t);
        }
    }
}
