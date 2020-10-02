package com.rabi.functional;

import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.rabi.Config;
import com.rabi.DBFactory;
import com.rabi.TestConfig;
import com.rabi.Util;
import com.rabi.internal.db.DBImpl;
import com.rabi.internal.db.engine.EngineImpl;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.memtable.MemTableImpl;
import com.rabi.internal.db.engine.task.boot.BaseTask;
import com.rabi.internal.db.engine.wal.Record;
import com.rabi.internal.db.engine.wal.Segment;
import com.rabi.internal.db.engine.wal.WalImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EngineTest {

  private static final Logger LOG = LoggerFactory.getLogger(EngineTest.class);

  /**
   * What do we test here:
   * - initially only 1 memtable is created and no immutables
   * - until num keys reaches limit above holds true
   * - on num key reaching(even from multiple threads), there is just 1 immutable table
   * which is the old mutable table and a new mutable table is created
   */
  @Test
  void testBootstrap(@TempDir final Path dataDir) throws CloneNotSupportedException, IllegalAccessException, ExecutionException, InterruptedException {
    LOG.info("\n\n**** testBootstrap *******\n\n");
    final DBImpl testDB = (DBImpl) DBFactory.getInstance(dataDir.toString(), LOG);
    final Config cfg = new Config.ConfigBuilder().setMemtableMaxKeys(1000).build();
    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, TimeUnit.SECONDS);
    });
    LOG.info("Engine started now in testBootstrap");
    /*
    Now DB is open, so check:
    - engine is RUNNING
    - no immutable tables
    - 1 mutable table which is empty
    - no L2 and L3 artifacts
     */
    final Field engineField = getField(testDB.getClass(), "engine", true);
    final EngineImpl engine = (EngineImpl) engineField.get(testDB);
    final Field engineStateField = getField(engine.getClass(), "state", true);
    final Field immutableTablesField = getField(engine.getClass(), "immutableTables", true);
    final Field l2IndexesField = getField(engine.getClass(), "l2Indexes", true);
    final Field l3IndexesField = getField(engine.getClass(), "l3Indexes", true);
    final Field l2DataField = getField(engine.getClass(), "l2Data", true);
    final Field l3DataField = getField(engine.getClass(), "l3Data", true);
    final Field mutableTableField = getField(engine.getClass(), "mutableTable", true);

    assertEquals("RUNNING", engineStateField.get(engine).toString());
    assertEquals(0, ((ConcurrentLinkedDeque<MemTable>) immutableTablesField.get(engine)).size());
    assertEquals(0, ((Map<Long, Index>) l2IndexesField.get(engine)).size());
    assertEquals(0, ((Map<Long, Index>) l3IndexesField.get(engine)).size());
    assertEquals(0, ((Map<Long, Index>) l2DataField.get(engine)).size());
    assertEquals(0, ((Map<Long, Index>) l3DataField.get(engine)).size());
    assertEquals(0, ((MemTableImpl) mutableTableField.get(engine)).size()); // interface casted to implementation
    testDB.stop().get();
  }

  @Test
  void testPutRotates(@TempDir final Path dataDir) throws IllegalAccessException, InterruptedException, ExecutionException, IOException {
    LOG.info("\n\n**** testPutRotates *******\n\n");
    final int MEMTABLE_MAX_KEYS = 1000;
    final int NUM_THREADS = Runtime.getRuntime().availableProcessors();

    final DBImpl testDB = (DBImpl) DBFactory.getInstance(dataDir.toString(), LOG);
    final Config cfg = new Config.ConfigBuilder().setMemtableMaxKeys(MEMTABLE_MAX_KEYS).build();
    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, TimeUnit.SECONDS);
    });
    LOG.info("Engine started now in testPutRotates");

    final Field engineField = getField(testDB.getClass(), "engine", true);
    final EngineImpl engine = (EngineImpl) engineField.get(testDB);
    final Field immutableTablesField = getField(engine.getClass(), "immutableTables", true);
    final Field mutableTableField = getField(engine.getClass(), "mutableTable", true);
    final Field flusherField = getField(engine.getClass(), "flusher", true);
    final Field compactorField = getField(engine.getClass(), "compactor", true);
    final Field eventListenerField = getField(engine.getClass(), "eventListener", true);

    /*
    Stop these routines so that we just dont have flushing as well as compaction
     */
    final Thread flusher = (Thread) flusherField.get(engine);
    final Thread compactor = (Thread) compactorField.get(engine);
    final Thread eventListener = (Thread) eventListenerField.get(engine);
    Util.doInterruptThread(eventListener);
    Util.doInterruptThread(compactor);
    Util.doInterruptThread(flusher);

    /*
    Bombard with lots of PUTs from multiple threads.
    We will write 3.5 * MEMTABLE_MAX_KEYS keys and this should lead to:
    - (N>1) immutable tables and 1 mutable table with combined size as num keys.
    -  N + 1 WALs with num records same as total keys and size of each WAL as memtable
    - flusher signalled
     */
    final int totalKeys = (int) (3.5 * MEMTABLE_MAX_KEYS);
    final int keysPerThread = totalKeys / NUM_THREADS;
    final byte[] val = "the123saurav".getBytes();
    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    final Map<ByteArrayWrapper, ByteArrayWrapper> inputKeys = new ConcurrentHashMap<>();

    LOG.info("Starting to prime the DB with total:{}, perThread: {}", totalKeys, keysPerThread);
    for (int thread = 0; thread < NUM_THREADS; ++thread) {
      executor.submit(() -> {
        try {
          // Let all threads bombard together
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          e.printStackTrace();
        }
        for (int i = 0; i < keysPerThread; ++i) {
          final byte[] key = Util.getAlphaNumericString(30).getBytes();
          try {
            testDB.put(key, val);
            inputKeys.put(new ByteArrayWrapper(key), new ByteArrayWrapper(val));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
    final StopWatch stopWatch = StopWatch.createStarted();
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);
    stopWatch.stop();
    LOG.info("Done priming the database in {} ms", stopWatch.getTime());

    final ConcurrentLinkedDeque<MemTable> immutables = (ConcurrentLinkedDeque<MemTable>) immutableTablesField.get(engine);
    final MemTable mutable = (MemTable) mutableTableField.get(engine);
    final long actualKeys = immutables.stream().reduce(0L, (acc, e) -> acc + e.size(), Long::sum) + mutable.size();
    assertTrue(immutables.size() > 0);
    assertEquals(totalKeys, actualKeys);

    final Map<Long, Long> memTables = new HashMap<>();
    memTables.put(mutable.getId(), mutable.size());
    immutables.stream().forEach(im -> memTables.put(im.getId(), im.size()));

    Function<Path, Long> segmentMapper = p -> {
      final String[] pathPieces = p.toString().split("/");
      final String fileName = pathPieces[pathPieces.length - 1];
      return Long.valueOf(fileName.split("\\.")[0]);
    };
    Stream<Path> paths = Files.walk(dataDir);

    Map<Long, List<Path>> walToSegments = paths.
        filter(p -> p.toString().endsWith(".wal")).
        collect(Collectors.groupingBy(segmentMapper, Collectors.toList()));

    assertEquals(walToSegments.size(), memTables.size());

    final Map<Long, Map<ByteArrayWrapper, ByteArrayWrapper>> walToRecords = new HashMap<>();
    walToSegments.forEach((walID, segPaths) -> {
      final List<Segment> segments = segPaths.stream().map(p -> new Segment(p, false)).collect(Collectors.toList());
      final Segment[] segs = segments.toArray(new Segment[0]);
      final List<Record> walRecords = new WalImpl(walID, segs, LOG).load();
      final Map<ByteArrayWrapper, ByteArrayWrapper> walEntries = new HashMap<>();
      walRecords.stream().forEach(r -> walEntries.put(new ByteArrayWrapper(r.getKey()), new ByteArrayWrapper(r.getVal())));
      walToRecords.put(walID, walEntries);
    });

    /*
    There should be a WAL corresponding to each memtable and keys count should match.
    This also asserts WAL keys == totalKeys
     */
    memTables.forEach((id, numKeys) -> assertEquals(numKeys, walToRecords.get(id).size()));

    walToRecords.values().forEach(map -> map.forEach((k, v) -> assertEquals(v, inputKeys.get(k))));

    final Field toFlusherField = getField(engine.getClass(), "toFlusher", true);

    final BlockingQueue toFlusher = (BlockingQueue) toFlusherField.get(engine);
    assertEquals(immutables.size(), toFlusher.size());

    testDB.stop().get();
  }
}
