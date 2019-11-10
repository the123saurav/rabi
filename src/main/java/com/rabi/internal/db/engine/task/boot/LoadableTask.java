package com.rabi.internal.db.engine.task.boot;

import com.rabi.internal.db.engine.Loadable;
import org.slf4j.Logger;

public class LoadableTask extends BaseTask {

    private final Logger log;
    private final Loadable task;

    public LoadableTask(Loadable t, Logger l) {
        log = l;
        task = t;
    }

    @Override
    public void run() {
        log.debug("Running LoadableTask: " + task);
        task.load();
    }
}
