package com.rabi.internal.db.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabi.Config;
import com.rabi.exceptions.InitialisationException;
import com.rabi.exceptions.InvalidDBStateException;
import com.rabi.exceptions.WritesStalledException;
import com.rabi.internal.db.Engine;
import com.rabi.internal.db.engine.channel.Message;
import com.rabi.internal.db.engine.channel.message.EngineToFlusher;
import com.rabi.internal.db.engine.channel.message.FlusherToEngine;
import com.rabi.internal.db.engine.flusher.FlusherImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.db.engine.memtable.MemTableImpl;
import com.rabi.internal.db.engine.task.boot.BaseTask;
import com.rabi.internal.db.engine.task.boot.FileCleanupTask;
import com.rabi.internal.db.engine.task.boot.LoadableTask;
import com.rabi.internal.db.engine.util.HaltingFixedThreadPoolExecutor;
import com.rabi.internal.db.engine.wal.Segment;
import com.rabi.internal.db.engine.wal.WalImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;

public class EngineImpl implements Engine {

  private final static String WAL_SUFFIX = ".wal";
  private final static String L2_INDEX_SUFFIX = ".l2.index";
  private final static String L3_INDEX_SUFFIX = ".l3.index";
  private final static String DATA_SUFFIX = ".data";
  private final static String TMP_SUFFIX = ".tmp";

  private enum FileType {
    WAL, DATA, L2INDEX, L3INDEX, TMP
  }

  private final Path dataDir;
  private final Config cfg;
  private final Logger log;
  private final long maxKeys;
  private final ReentrantLock mutLock;
  private final ConcurrentLinkedDeque<MemTable> immutableTables;
  private final List<Index> l2Indexes;
  private final List<Index> l3Indexes;
  private final BlockingQueue<EngineToFlusher> toFlusher;
  private final BlockingQueue<Message> fromFlusher;

  private class MessageListener implements Runnable {

    @Override
    public void run() {
      log.info("Starting engine EventListener...");
      while (true) {
        Message m;
        try {
          m = fromFlusher.take();
        } catch (InterruptedException e) {
          log.warn("EventListener interrupted, exiting...");
          return;
        }
        if (m instanceof FlusherToEngine) {
          try {
            handleFlusherMessage((FlusherToEngine) m);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    private void handleFlusherMessage(FlusherToEngine e) throws IOException {
      log.info("received message from Flusher: {}", e.getIndex());
      l2Indexes.add(e.getIndex());
      MemTable m = immutableTables.getLast();
      immutableTables.removeLast();
      log.info("removing wal");
      m.cleanup();
    }
  }

  // mutables
  private volatile State state;
  private volatile MemTable mutableTable;
  private Thread flusher;
  private Thread eventListener;

  public EngineImpl(String dDir, Config cfg, Logger logger) {
    this.dataDir = Paths.get(dDir);
    this.cfg = cfg;
    this.log = logger;
    this.state = State.INITIALIZED;
    this.maxKeys = cfg.getMemtableMaxKeys();
    this.mutLock = new ReentrantLock();
    this.immutableTables = new ConcurrentLinkedDeque<>();
    this.l2Indexes = new ArrayList<>();
    this.l3Indexes = new ArrayList<>();
    this.toFlusher = new LinkedBlockingQueue<>();
    this.fromFlusher = new LinkedBlockingQueue<>();
    logger.debug("memtable max keys: " + this.maxKeys);
  }

  /**
   * At boot-up we have number of activities to do which can be parallelized.
   * 1. cleanup
   * 2. read WAL and populate memtables.
   * 3. load l2 index(parallelism upto number of L2 files)
   * 4. load l3 index(parallelism upto number of L3 files)
   * 5. start runtime routines
   * The engine first gathers number of independent tasks.
   * <p>
   * Note that only 1,2,3,4 are in purview of parallelism.
   */
  @Override
  public synchronized void start() {
    log.info("starting DB engine");
    this.state = State.BOOTING;

    //create data dir if not exist
    try {
      Files.createDirectories(dataDir);
    } catch (IOException e) {
      throw new InitialisationException(e);
    }

    Function<Path, FileType> fileTypeMapper = p -> {
      final String pathStr = p.toString();
      if (pathStr.endsWith(WAL_SUFFIX)) {
        return FileType.WAL;
      } else if (pathStr.endsWith(L2_INDEX_SUFFIX)) {
        return FileType.L2INDEX;
      } else if (pathStr.endsWith(L3_INDEX_SUFFIX)) {
        return FileType.L3INDEX;
      } else {
        return FileType.TMP;
      }
    };

    //this is not working.
    Map<FileType, List<Path>> fileMap;

    try (Stream<Path> files = Files.walk(dataDir)) {
      fileMap = files.filter(e -> {
        final String pathStr = e.toString();
        return pathStr.endsWith(WAL_SUFFIX)
            || pathStr.endsWith(L2_INDEX_SUFFIX)
            || pathStr.endsWith(L3_INDEX_SUFFIX)
            || pathStr.endsWith(TMP_SUFFIX);
      }).collect(Collectors.groupingBy(fileTypeMapper, Collectors.toList()));

    } catch (IOException e) {
      //no need to cleanup
      throw new InitialisationException(e);
    }

    List<BaseTask> tasks = getBootTasks(fileMap);

    log.debug("tasks to load at boot: {}", tasks);
    if (tasks.size() > 0) {
      int parallelism = cfg.getBootParallelism();
      boolean errInInit = false;
      if (parallelism > 1) {
        HaltingFixedThreadPoolExecutor ex = new HaltingFixedThreadPoolExecutor(parallelism,
            new ThreadFactoryBuilder().setDaemon(true).
                setNameFormat("boot-task-%d").build()
        );
        tasks.stream().forEach(ex::submit);
        ex.shutdown();
        try {
          ex.awaitTermination(600, TimeUnit.SECONDS);
          log.info("Processed all boot tasks...");
        } catch (final InterruptedException e) {
          errInInit = true;
        }
        if (errInInit || ex.didWeErr()) {
          throw new InitialisationException("error in initialisation");
        }
      } else {
        tasks.stream().forEach(t -> {
          try {
            t.run();
          } catch (Exception e) {
            e.printStackTrace();
            throw new InitialisationException(e);
          }
        });
      }
    }

    //if there is even a single WAL, mutableTable is set
    if (mutableTable == null) {
      log.debug("setting mutable memtable.");
      mutableTable = newTable(Instant.now().toEpochMilli());
      mutableTable.load();
    }

    //start runtime routines below
    eventListener = new Thread(new MessageListener());
    flusher = new Thread(new FlusherImpl(toFlusher, fromFlusher));

    eventListener.start();
    flusher.start();
    if (immutableTables.size() > 0) {
      toFlusher.add(new EngineToFlusher(immutableTables.getLast(), cfg.getSync(), dataDir));
    }
    this.state = State.RUNNING;
    log.info("Engine is running now.");
  }

  /**
   * Threadsafe
   * <p>
   * will keep crashing, hence we can stall writes at below conditions:
   * - max_memtables
   * - memory available is less(triggers flushing if not already)
   * - max_L2_files reached (compaction is slower than rate at which flushing is taking place)
   *
   * @return
   */
  private boolean writesStalled() {
    return immutableTables.size() >= cfg.getMaxMemtables() ||
        l2Indexes.size() >= cfg.getMaxFlushedFiles();
  }

  /**
   * in call to put, check writes stalled or not.
   * if not stalled:
   * insert into table.
   * if(size full){
   * tryLock();
   * //for 1st guy, go and create new table, others can exit knowing someone is there,
   * even if below fails, someone will detect size full next time and start again
   * if lock succeeds:
   * // create new memtable with numsegemnts from cfg.
   * // get curr memtable
   * // update curr memtable
   * // put curr to immutable(dont disallowmutation yet OR close wal for writes, we do it during flushing)
   * // is there a way for us to decide if all writes would
   * // now see new table only, maybe if this table is non writable, reread ref and retry.
   * <p>
   * }
   *
   * @param k - key
   * @param v - value
   * @throws IOException
   */
  @Override
  public void put(byte[] k, byte[] v) throws IOException {
    if (state == State.TERMINATED || state == State.TERMINATING) {
      throw new InvalidDBStateException("DB is Terminated");
    }
    if (writesStalled()) {
      throw new WritesStalledException();
    }

    //TODO: maybe check if mutation disallowed exception and retry
    mutableTable.put(k, v);
    if (mutableTable.size() > maxKeys && mutLock.tryLock()) {
      if (mutableTable.size() > maxKeys) { //DCL
        log.info("memtable full, rotating.");
        MemTable newTable = newTable(Instant.now().toEpochMilli());
        newTable.load(); //creates segments
        immutableTables.add(mutableTable);
        mutableTable = newTable;
        log.info("signalling flusher...");
        toFlusher.add(new EngineToFlusher(immutableTables.getLast(), cfg.getSync(), dataDir));
        mutLock.unlock();
      }
    }
  }

  @Override
  public void delete(byte[] k) throws IOException {
    if (state == State.TERMINATED || state == State.TERMINATING) {
      throw new InvalidDBStateException("Terminated");
    }
    if (writesStalled()) {
      throw new WritesStalledException();
    }

    //TODO: maybe check if mutation disallowed exception and retry
    mutableTable.delete(k);
    if (mutableTable.size() > maxKeys && mutLock.tryLock()) {
      if (mutableTable.size() > maxKeys) {
        log.debug("memtable full, rotating.");
        MemTable newTable = newTable(Instant.now().toEpochMilli());
        newTable.load(); //creates segments
        immutableTables.add(mutableTable);
        mutableTable = newTable;
      }
      mutLock.unlock();
    }
  }

  /**
   * We:
   * - set state to TERMINATING to disallow any ops.
   * - release file locks if any.
   * - flush WAL segments(if sync is off) and close write channel(to prevent corruption,
   * we are doing it here)
   * - if mode == GRACEFUL:
   * - invoke flushing routine to write to disk(data and index file) and
   * rename WAL file for immutable memtables.
   * - no need to flush mutable memTable.
   * - set state == TERMINATED.
   */
  @Override
  public synchronized void shutdown() throws IOException {
    if (state == State.TERMINATED) {
      return;
    }
    state = State.TERMINATING; //marker that somebody invoked shutdown
    mutableTable.close();
    if (cfg.getShutdownMode() == Config.ShutdownMode.GRACEFUL) {

    }
    state = State.TERMINATED;
  }

  private List<BaseTask> getBootTasks(final Map<FileType, List<Path>> m) {
    final List<BaseTask> tasks = new ArrayList<>();
    m.forEach((t, p) -> {
      switch (t) {
        case TMP:
          tasks.addAll(getTmpFileTasks(p));
          break;
        case WAL:
          tasks.addAll(getMemtableTasks(p));
          break;
        case L2INDEX:
          tasks.addAll(getL2IndexTasks(p));
          break;
        case L3INDEX:
          tasks.addAll(getL3IndexTasks(p));
          break;
      }
    });
    return tasks;
  }

  private List<BaseTask> getTmpFileTasks(final List<Path> paths) {
    final List<BaseTask> tasks = new ArrayList<>();
    paths.forEach(p -> tasks.add(new FileCleanupTask(p)));
    return tasks;
  }

  private List<BaseTask> getMemtableTasks(final List<Path> paths) {
    //we have all WAL segment files here, need to partition it.
    Function<Path, Long> segmentMapper =
        p -> {
          log.debug("getMemtableTasks: p is {}", p.getFileName().toString().split("\\."));
          return Long.valueOf(p.getFileName().toString().split("\\.")[0]);
        };

    final List<BaseTask> tasks = new ArrayList<>();

    Map<Long, List<Path>> walToSegments = paths.stream()
        .collect(Collectors.groupingBy(segmentMapper, Collectors.toList()));

    //the highest timestamp wal becomes mutable
    long highestTS = Collections.max(walToSegments.keySet());
    walToSegments.forEach((ts, p) -> {
      final MemTable m = newTable(ts, p.size());
      tasks.add(new LoadableTask(m, log));
      if (highestTS == ts) {
        log.info("setting: " + ts + " as mutable table");
        mutableTable = m;
      } else {
        immutableTables.add(m);
      }
    });
    return tasks;
  }

  private MemTable newTable(long ts) {
    return newTable(ts, cfg.getMemtableSegments());
  }

  private MemTable newTable(long ts, int numSeg) {
    Segment[] segs = new Segment[numSeg];
    for (int i = 0; i < numSeg; ++i) {
      segs[i] = new Segment(Paths.get(dataDir.toString() + "/" + ts + "." + i + "." + "wal"),
          cfg.getSync());
    }
    return new MemTableImpl(new WalImpl(ts, segs, log), ts, log);
  }

  private List<BaseTask> getL2IndexTasks(List<Path> paths) {
    return paths.stream().map(p -> {
      final Loadable l = new IndexImpl.IndexLoader(p);
      return new LoadableTask<Index>(l, log, l2Indexes::add);
    }).collect(Collectors.toList());
  }

  private List<BaseTask> getL3IndexTasks(List<Path> paths) {
    return paths.stream().map(p -> {
      Loadable l = new IndexImpl.IndexLoader(p);
      return new LoadableTask<Index>(l, log, l3Indexes::add);
    }).collect(Collectors.toList());
  }
}
