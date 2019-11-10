package com.rabi.internal.db.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabi.Config;
import com.rabi.exceptions.InitialisationException;
import com.rabi.exceptions.InvalidDBStateException;
import com.rabi.exceptions.WritesStalledException;
import com.rabi.internal.db.Engine;
import com.rabi.internal.db.engine.filter.FilterImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.db.engine.memtable.MemTableImpl;
import com.rabi.internal.db.engine.task.boot.BaseTask;
import com.rabi.internal.db.engine.task.boot.FileCleanupTask;
import com.rabi.internal.db.engine.task.boot.LoadableTask;
import com.rabi.internal.db.engine.util.HaltingFixedThreadPoolExecutor;
import com.rabi.internal.db.engine.wal.Segment;
import com.rabi.internal.db.engine.wal.WalImpl;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EngineImpl implements Engine {

    private static String WAL_SUFFIX = ".wal";
    private static String L2_INDEX_SUFFIX = ".l2.index";
    private static String L3_INDEX_SUFFIX = ".l3.index";
    private static String DATA_SUFFIX = ".data";
    private static String TMP_SUFFIX = ".tmp";

    private enum FileType {
        WAL, DATA, L2INDEX, L3INDEX, TMP
    }

    private final Path dataDir;
    private final Config cfg;
    private final Logger log;
    private volatile State state;

    private ConcurrentLinkedDeque<MemTable> immutableTables = new ConcurrentLinkedDeque<>();
    private MemTable mutableTable; //created on first put
    private List<Index> l2Indexes = new ArrayList<>();
    private List<Filter> l3Filters = new ArrayList<>();

    public EngineImpl(String dDir, Config cfg, Logger logger) {
        this.dataDir = Paths.get(dDir);
        this.cfg = cfg;
        this.log = logger;
        this.state = State.INITIALZED;
    }

    /**
     * At boot-up we have number of activities to do which can be parallelized.
     * 1. cleanup
     * 2. read WAL and populate memtables.
     * 3. load l1 index(parallelism upto number of L1 files)
     * 4. populate l2 bloom(parallelism upto number of L2 files)
     * 5. start runtime routines
     * The engine first gathers number of independent task.
     * <p>
     * Note that only 1,2,3,4 are in purview of parallelism.
     */
    @Override
    public synchronized void start() {
        log.info("starting DB engine");
        this.state = State.BOOTING;

        //create dir if not exist
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new InitialisationException(e);
        }

        Function<Path, FileType> fileTypeMapper = p -> {
            if (p.toString().endsWith(WAL_SUFFIX)) {
                return FileType.WAL;
            } else if (p.toString().endsWith(L2_INDEX_SUFFIX)) {
                return FileType.L2INDEX;
            } else if (p.toString().endsWith(L3_INDEX_SUFFIX)) {
                return FileType.L3INDEX;
            } else {
                return FileType.TMP;
            }
        };

        //this is not working.
        Map<FileType, List<Path>> fileMap;
        try (Stream<Path> files = Files.walk(dataDir)) {
            fileMap = files.filter(
                    e -> e.toString().endsWith(WAL_SUFFIX)
                            || e.toString().endsWith(L2_INDEX_SUFFIX)
                            || e.toString().endsWith(L3_INDEX_SUFFIX)
                            || e.toString().endsWith(TMP_SUFFIX))
                    .collect(Collectors.groupingBy(fileTypeMapper, Collectors.toList()));

        } catch (IOException e) {
            //no need to cleanup
            throw new InitialisationException(e);
        }

        List<BaseTask> tasks = getTasks(fileMap);

        log.debug(tasks + " tasks to load at boot.");
        if (tasks.size() > 0) {

            int parallelism = cfg.getBootParallelism();
            boolean errInInit = false;
        /*
        If we throw any error here, we should
        initiate engine shutdown. Make sure to
        - close files
        - release file lock
        - GC memory
         */
            if (parallelism > 1) {
                HaltingFixedThreadPoolExecutor ex = new HaltingFixedThreadPoolExecutor(parallelism,
                        new ThreadFactoryBuilder().setDaemon(true).
                                setNameFormat("boot-task-%d").build()
                );
                tasks.stream().forEach(ex::submit);
                ex.shutdown();
                try {
                    ex.awaitTermination(600, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
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
            log.debug("setting mutable memtable");
            mutableTable = newTable(Instant.now().toEpochMilli());
            mutableTable.load();
        }

        //start runtime routines below

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
        return immutableTables.size() >= cfg.getMaxMemtables() || l2Indexes.size() >= cfg.getMaxFlushedFiles();
    }

    /**
     * in call to put, check writes stalled or not.
     * if not stalled:
     * insert into table.
     * if(size full){
     * tryLock();
     * //for 1st guy, go and create new table, others can exit knowing someone is there,
     * even if below fails, someone will detect size full and start again
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
            throw new InvalidDBStateException("Terminated");
        }
        if (writesStalled()) {
            throw new WritesStalledException();
        }

        mutableTable.put(k, v);
    }

    @Override
    public void delete(byte[] k) {

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

    private List<BaseTask> getTasks(Map<FileType, List<Path>> m) {
        List<BaseTask> tasks = new ArrayList<>();
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
                    tasks.addAll(getL3FilterTasks(p));
                    break;
            }
        });
        return tasks;
    }

    private List<BaseTask> getTmpFileTasks(List<Path> paths) {
        List<BaseTask> tasks = new ArrayList<>();
        paths.forEach(p -> tasks.add(new FileCleanupTask(p, log)));
        return tasks;
    }

    private List<BaseTask> getMemtableTasks(List<Path> paths) {
        //we have all WAL segment files here, need to partition it.
        Function<Path, Long> segmentMapper =
                p -> {
                    log.debug("getMemtableTasks: p is " + p.getFileName().toString().split("\\."));
                    return Long.valueOf(p.getFileName().toString().split("\\.")[0]);
                };

        List<BaseTask> tasks = new ArrayList<>();

        Map<Long, List<Path>> walToSegments = paths.stream()
                .collect(Collectors.groupingBy(segmentMapper, Collectors.toList()));

        //the highest timestamp wal becomes mutable
        long highestTS = Collections.max(walToSegments.keySet());
        walToSegments.forEach((ts, p) -> {
            MemTable m = newTable(ts, p.size());
            tasks.add(new LoadableTask(m, log));
            if (highestTS == ts) {
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
            segs[i] = new Segment(Paths.get(dataDir.toString() + "/" + ts + "." + i + "." + "wal"), cfg.getSync(), log);
        }
        return new MemTableImpl(new WalImpl(ts, segs, log), log);
    }

    private List<BaseTask> getL2IndexTasks(List<Path> paths) {
        return paths.stream().map(p -> {
            Index i = new IndexImpl(p);
            l2Indexes.add(i);
            return new LoadableTask(i, log);
        }).collect(Collectors.toList());
    }

    private List<BaseTask> getL3FilterTasks(List<Path> paths) {
        return paths.stream().map(p -> {
            Filter f = new FilterImpl(p);
            l3Filters.add(f);
            return new LoadableTask(f, log);
        }).collect(Collectors.toList());
    }
}
