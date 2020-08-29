package com.rabi.internal.db.engine.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HaltingFixedThreadPoolExecutor extends ThreadPoolExecutor {

  private volatile boolean didErr;

  public HaltingFixedThreadPoolExecutor(int nThreads, ThreadFactory t) {
    super(nThreads, nThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        t);
  }

  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    //Futures swallow exception and hence t would be null. Uncomment below when
    // we submit callables.
          /*if (t == null && r instanceof Future<?>) {
            try {
              Object result = ((Future<?>) r).get();
            } catch (CancellationException ce) {
                t = ce;
            } catch (ExecutionException ee) {
                t = ee.getCause();
            } catch (InterruptedException ie) {
                // maybe just mark to interrupt but still set t to stop others
                Thread.currentThread().interrupt(); // ignore/reset
            }
          }*/
    if (t != null) {
      didErr = true;
      // Error in any task should shut down the executor.
      if (!isTerminating()) {
        shutdown();
      }
    }
  }

  public boolean didWeErr() {
    return didErr;
  }
}
