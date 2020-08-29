package com.rabi.internal.db.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabi.Config;
import com.rabi.exceptions.InitialisationException;
import com.rabi.exceptions.InvalidDBStateException;
import com.rabi.exceptions.WritesStalledException;
import com.rabi.internal.db.Engine;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.db.engine.memtable.MemTableImpl;
import com.rabi.internal.db.engine.task.boot.BaseTask;
import com.rabi.internal.db.engine.task.boot.FileCleanupTask;
import com.rabi.internal.db.engine.task.boot.LoadableTask;
import com.rabi.internal.db.engine.util.AppUtils;
import com.rabi.internal.db.engine.util.FileUtils;
import com.rabi.internal.db.engine.util.HaltingFixedThreadPoolExecutor;
import com.rabi.internal.db.engine.wal.Segment;
import com.rabi.internal.db.engine.wal.WalImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EngineImpl implements Engine {

    private final static String WAL_SUFFIX = ".wal";
    private final static String L2_INDEX_SUFFIX = ".l2.index";
    private final static String L3_INDEX_SUFFIX = ".l3.index";
    private final static String L2_DATA_SUFFIX = "l2.data";
    private final static String L3_DATA_SUFFIX = "l3.data";
    private final static String TMP_SUFFIX = ".tmp";

    private enum FileType {
        WAL, L2DATA, L2INDEX, L3DATA, L3INDEX, TMP
    }

    private final Path dataDir;
    private final Config cfg;
    private final Logger log;
    private final long maxKeys;
    private final ReentrantLock mutLock;
    private final ConcurrentLinkedDeque<MemTable> immutableTables;
    private final Map<Long, Index> l2Indexes;
    private final Map<Long, Data> l2Data;
    private final Map<Long, Index> l3Indexes;
    private final Map<Long, Data> l3Data;
    private final BlockingQueue<EngineToFlusher> toFlusher;
    private final BlockingQueue<EngineToCompactor> toCompactor;
    private final BlockingQueue<Message> messageBus;

    private static class FlusherToEngine extends Message {
        private final Index index;

        FlusherToEngine(Index i){
            index = i;
        }

        Index getIndex(){
            return index;
        }
    }

    private class EngineToFlusher extends Message {
        private final MemTable m;

        EngineToFlusher(MemTable m){
            this.m = m;
        }

        MemTable getMemTable(){
            return m;
        }

        boolean getSyncMode(){ return cfg.getSync(); }

        Path getDataDir(){ return dataDir; }
    }

    private static class CompactorToEngine extends Message {}

    private static class EngineToCompactor extends Message {}

     private class Flusher implements Runnable {
        private final Logger log = LoggerFactory.getLogger(Flusher.class);

        @Override
        public void run() {
            log.info("Flusher started.");
            EngineToFlusher msg;
            while (true) {
                try {
                    msg = toFlusher.take(); //block for task
                    log.info("Got request to flush memtable.");
                    // engine adds the index and removes immutable table.
                    final Index i = doFlush(msg.getMemTable(), msg.getDataDir(), msg.getSyncMode());
                    log.info("Flushed memtable to disk at: {}", i.getPath());
                    messageBus.add(new FlusherToEngine(i));
                } catch (InterruptedException e) {
                    log.info("Shutting down routine...");
                    return;
                }
            }
        }

        //TODO: add expo backoff
        private Index doFlush(final MemTable m, final Path dataDir, final boolean syncMode) throws InterruptedException {
            while (true){
                try {
                    return flush(m, dataDir, syncMode);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * - get exported entryset
         * - create tmp data and index file
         * - dump to above file in batches
         * - create index file
         * - rename .tmp
         */
        private Index flush(final MemTable m, final Path dataDir, final boolean syncMode) throws IOException {
            final long id = m.getId();
            final List<Pair<byte[], byte[]>> recordSet = m.export();
            log.info("Flushing memtable: {} with {} records", id, m.size());
            final Data d = flushDataFile(recordSet, dataDir, id, syncMode);
            final Index i = flushIndexFile(recordSet, dataDir, id, syncMode);
            d.rename(Paths.get(dataDir.toString() + "/" + id + ".l2.data"));
            i.rename(Paths.get(dataDir.toString() + "/" + id + ".l2.index"));
            log.info("Renamed data and index files after flush");
            return i;
        }

        private Data flushDataFile(final List<Pair<byte[], byte[]>> recordSet,
                                   final Path dataDir, final long id, final boolean syncMode) throws IOException {
            final Path p = Paths.get(dataDir.toString() + "/" + id + ".l2.data.tmp");
            log.info("Data file path during flush would be: {}", p);
            final Data d = new DataImpl(p, FileUtils.getId(p));
            d.flush(recordSet, syncMode);
            log.info("Flushed datafile {} to {}", id, p);
            return d;
        }

        private Index flushIndexFile(final List<Pair<byte[], byte[]>> entries, Path dataDir, long id, boolean syncMode) throws IOException {
            byte[] tmp = new byte[256];
            Arrays.fill(tmp, (byte)255);
            ByteArrayWrapper minKey = new ByteArrayWrapper(tmp);
            ByteArrayWrapper maxKey = new ByteArrayWrapper(new byte[]{(byte)0});
            long minKeyOffset = 0;
            long maxKeyOffset = 0;
            Map<ByteArrayWrapper, Long> m = new HashMap<>();
            ByteArrayWrapper k;
            long fileOffset = 0;
            long currOffset;

            for(final Pair<byte[], byte[]> e: entries){
                k = new ByteArrayWrapper(e.getLeft());
                currOffset = 0;
                if(e.getRight() != null) {
                    currOffset = fileOffset;
                    fileOffset += 1 + 2 + e.getLeft().length + e.getRight().length;
                    //minkey/maxkey is one of the keys in data file
                    if (k.compareTo(minKey) <= 0) {
                        minKey = k;
                        minKeyOffset = currOffset;
                    }
                    if (k.compareTo(maxKey) >= 0) {
                        maxKey = k;
                        maxKeyOffset = currOffset;
                    }
                }
                m.put(k, currOffset);
            }
            final Path indexPath = Paths.get(dataDir.toString() + "/" + id + ".l2.index.tmp");
            log.info("Index file path upon flush would be: {}", indexPath);
            final long indexId = FileUtils.getId(indexPath);
            final Index i = IndexImpl.loadedIndex(indexPath, indexId, m, minKey.unwrap(),
                    minKeyOffset, maxKey.unwrap(), maxKeyOffset);
            i.flush(syncMode);
            return i;
        }
    }

    /**
     * The compactor would remove one L2index.
     * It will also update the L3indexes which is okay as we are not changing
     * any references.
     * Note that serving reads from L3 target files is okay when compaction is running as
     * the only keys that are updated in the index(we look into index for answering reads)
     * are from L2 candidate's file and hence if any query comes for these, they would be answered by L2 file only and
     * not come to L3.
     * For any other key, the L3 index would be unaffected and so would be the data file where we are only appending,
     * so all existing offsets, as seen in the L3 index, are valid.
     */
    private class Compactor implements Runnable {
        private final Logger log = LoggerFactory.getLogger(Compactor.class);

        @Override
        public void run() {
            log.info("Compactor started.");
            while (true) {
                try {
                    toCompactor.take(); //block for task
                    log.info("Got request to compact");
                    // engine adds the index and removes immutable table.
                    int n  = compact();
                    log.info("Done compacting {} files", n);
                    messageBus.add(new CompactorToEngine());
                } catch (InterruptedException e) {
                    log.info("Shutting down routine...");
                    return;
                } catch (IOException e) {
                    throw new RuntimeException("Error in compaction", e);
                }
            }
        }

        private int compact() throws IOException {
            int n = 0;
            while(l2Indexes.size() > cfg.getMaxFlushedFiles()) {
                if (l3Indexes.size() == 0) {
                    log.info("No file in L3, using simple compaction strategy.");
                    final Pair<Data, Index> candidatePair = getHighestDensityL2File();
                    final Index candidateIndex = candidatePair.getRight();
                    final Data candidateData = candidatePair.getLeft();
                    String currPath = candidateIndex.getPath().toString();
                    int l = currPath.lastIndexOf('/');
                    String head = currPath.substring(0, l) + "/";
                    String tail = currPath.substring(l + 1);
                    String newPath = head + tail.replace(".l2.", ".l3.");
                    try {
                        candidateIndex.lockAndSignal();
                        candidateIndex.rename(Paths.get(newPath));
                        currPath = candidateData.getPath().toString();
                        l = currPath.lastIndexOf('/');
                        head = currPath.substring(0, l) + "/";
                        tail = currPath.substring(l + 1);
                        newPath = head + tail.replace(".l2.", ".l3.");
                        candidateData.rename(Paths.get(newPath));
                        l3Indexes.put(candidateIndex.getId(), candidateIndex);
                        l3Data.put(candidateData.getID(), candidateData);
                        l2Indexes.remove(candidateIndex.getId());
                        l2Data.remove(candidateData.getID());
                        n++;
                    } finally {
                        candidateIndex.unlockAndSignal();
                    }
                } else {
                    log.info("Using regular compaction strategy");
                    // TODO
                }
            }
            return n;
        }

        private Pair<Data, Index> getHighestDensityL2File() {
            double maxDensity = Double.MIN_VALUE;
            Index maxDensityIndex = null;

            for(final Index i: l2Indexes.values()) {
                if(i.getDensity() > maxDensity) {
                    maxDensity = i.getDensity();
                    maxDensityIndex = i;
                }
            }
            final Data maxDensityData = l2Data.get(maxDensityIndex.getId());
            return new ImmutablePair<>(maxDensityData, maxDensityIndex);
        }
    }


        private class MessageListener implements Runnable {

        @Override
        public void run() {
            log.info("Starting engine MessageListener...");
            while (true) {
                Message m;
                try {
                    m = messageBus.take();
                } catch (final InterruptedException e) {
                    log.warn("EventListener interrupted, exiting...");
                    return;
                }
                if(m instanceof FlusherToEngine){
                    try {
                        handleFlusherMessage((FlusherToEngine)m);
                        if(l2Indexes.size() > cfg.getMaxFlushedFiles()) {
                            toCompactor.add(new EngineToCompactor());
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void handleFlusherMessage(final FlusherToEngine e) throws IOException {
            log.info("received message from Flusher: {}", e.getIndex());
            l2Indexes.put(e.getIndex().getId(), e.getIndex());
            final MemTable m = immutableTables.getLast();
            immutableTables.removeLast();
            log.info("removing wal for flushed memtable");
            m.cleanup();
        }
    }

    // mutables
    private volatile State state;
    private volatile MemTable mutableTable;
    private Thread flusher;
    private Thread compactor;
    private Thread eventListener;

    public EngineImpl(String dDir, Config cfg, Logger logger) {
        this.dataDir = Paths.get(dDir);
        this.cfg = cfg;
        this.log = logger;
        this.state = State.INITIALIZED;
        this.maxKeys = cfg.getMemtableMaxKeys();
        this.mutLock = new ReentrantLock();
        this.immutableTables = new ConcurrentLinkedDeque<>();
        this.l2Indexes = new HashMap<>();
        this.l2Data = new HashMap<>();
        this.l3Indexes = new HashMap<>();
        this.l3Data = new HashMap<>();
        this.toFlusher = new LinkedBlockingQueue<>();
        this.toCompactor = new LinkedBlockingQueue<>();
        this.messageBus = new LinkedBlockingQueue<>();
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
            } else if(pathStr.endsWith(L2_DATA_SUFFIX)) {
                return FileType.L2DATA;
            } else if(pathStr.endsWith(L3_DATA_SUFFIX)) {
                return FileType.L3DATA;
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
                        || pathStr.endsWith(L2_DATA_SUFFIX)
                        || pathStr.endsWith(L3_DATA_SUFFIX)
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
            mutableTable.boot();
        }

        //start runtime routines below
        eventListener = new Thread(new MessageListener());
        eventListener.start();

        flusher = new Thread(new Flusher());
        flusher.start();
        if(immutableTables.size() > 0){
            log.info("Signalling flusher");
            toFlusher.add(new EngineToFlusher(immutableTables.getLast()));
        }

        compactor = new Thread(new Compactor());
        compactor.start();
        if(l2Indexes.size() > cfg.getMaxFlushedFiles()) {
            log.info("Signalling compactor");
            toCompactor.add(new EngineToCompactor());
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
        return immutableTables.size() >= cfg.getMaxMemtables() || l2Indexes.size() > cfg.getMaxFlushedFiles();
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
            if(mutableTable.size() > maxKeys) { //DCL
                log.info("memtable full, rotating.");
                MemTable newTable = newTable(Instant.now().toEpochMilli());
                newTable.boot(); //creates segments
                immutableTables.add(mutableTable);
                mutableTable = newTable;
                log.info("signalling flusher...");
                toFlusher.add(new EngineToFlusher(immutableTables.getLast()));
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
                    newTable.boot(); //creates segments
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
                case L2DATA:
                    tasks.addAll(getL2DataTasks(p));
                    break;
                case L3DATA:
                    tasks.addAll(getL3DataTasks(p));
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
            segs[i] = new Segment(Paths.get(dataDir.toString() + "/" + ts + "." + i + "." + "wal"), cfg.getSync());
        }
        return new MemTableImpl(new WalImpl(ts, segs, log), ts, log);
    }

    private List<BaseTask> getL2IndexTasks(List<Path> paths) {
        return paths.stream().map(p -> {
            final Bootable l = new IndexImpl.IndexLoader(p);
            return new LoadableTask<Index>(l, log, i -> l2Indexes.put(i.getId(), i));
        }).collect(Collectors.toList());
    }

    private List<BaseTask> getL3IndexTasks(List<Path> paths) {
        return paths.stream().map(p -> {
            Bootable l = new IndexImpl.IndexLoader(p);
            return new LoadableTask<Index>(l, log, i -> l3Indexes.put(i.getId(), i));
            }).collect(Collectors.toList());
    }

    private List<BaseTask> getL2DataTasks(final List<Path> paths) {
        return paths.stream().map(p -> {
            final Bootable l = new DataImpl.DataBooter(p);
            return new LoadableTask<Data>(l, log, d -> l2Data.put(d.getID(), d));
        }).collect(Collectors.toList());
    }

    private List<BaseTask> getL3DataTasks(final List<Path> paths) {
        return paths.stream().map(p -> {
            final Bootable l = new DataImpl.DataBooter(p);
            return new LoadableTask<Data>(l, log, d -> l3Data.put(d.getID(), d));
        }).collect(Collectors.toList());
    }
}
