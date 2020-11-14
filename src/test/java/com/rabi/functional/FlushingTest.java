package com.rabi.functional;

import static com.rabi.Util.loadDataFromDisk;
import static com.rabi.Util.loadIndexFromDisk;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.google.common.primitives.UnsignedBytes;
import com.rabi.Config;
import com.rabi.DBFactory;
import com.rabi.Util;
import com.rabi.internal.db.DBImpl;
import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.EngineImpl;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import com.rabi.unit.TestConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * During flushing we take an immutable memtable and convert it into L2 index and L2 data file.
 * It then notifies engine which updates its in memory structures.
 */
class FlushingTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlushingTest.class);

  /**
   * Here we open DB, disable compactor, then bombard with writes so that flushing is triggered.
   * Now we should see:
   * - data file and index file with expected name created
   * - index file header is correct
   * - index file has all records as memtable
   * - data file format is rightly stored in index file
   * - index + data file merging gives all records in MT
   *
   * @param dataDir
   */
  @Test
  void testFlush(@TempDir final Path dataDir) throws IllegalAccessException, InterruptedException, ExecutionException, IOException {
    LOG.info("\n\n**** testFlush *******\n\n");
    final int MEMTABLE_MAX_KEYS = 1000;

    final DBImpl testDB = (DBImpl) DBFactory.getInstance(dataDir.toString(), LOG);
    final Config cfg = new Config.ConfigBuilder().setMemtableMaxKeys(MEMTABLE_MAX_KEYS).build();
    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, SECONDS);
    });
    LOG.info("Engine started now in testFlush");

    // Let flusher and compactor start
    final Field engineField = getField(testDB.getClass(), "engine", true);
    final EngineImpl engine = (EngineImpl) engineField.get(testDB);
    final Field compactorField = getField(engine.getClass(), "compactor", true);
    final Thread compactor = (Thread) compactorField.get(engine);
    final Field flusherField = getField(engine.getClass(), "flusher", true);
    final Thread flusher = (Thread) flusherField.get(engine);
    final Field immutableTablesField = getField(engine.getClass(), "immutableTables", true);
    while (!flusher.isAlive() || !compactor.isAlive()) {
      Thread.sleep(500);
    }

    /*
    Stop these routines so that we just don't have compaction.
     */
    LOG.info("trying to shut down compactor");
    Util.doInterruptThread(compactor);
    Util.doInterruptThread(flusher);

    final ConcurrentLinkedDeque<MemTable> immutables = (ConcurrentLinkedDeque<MemTable>) immutableTablesField.get(engine);
    final byte[] val = "the123saurav".getBytes();
    for (int i = 0; i < 1010; ++i) { // enough to create 1 immutable table
      final byte[] key = Util.getAlphaNumericString(30).getBytes();
      try {
        testDB.put(key, val);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Done putting to DB");

    final Field toFlusherField = getField(engine.getClass(), "toFlusher", true);
    final BlockingQueue<EngineImpl.EngineToFlusher> toFlusher = (BlockingQueue<EngineImpl.EngineToFlusher>)
        toFlusherField.get(engine);
    EngineImpl.EngineToFlusher flushMessage = toFlusher.peek();
    assertNotNull(flushMessage);
    final MemTable immutable = flushMessage.getMemTable();
    assertNotNull(immutable);
    final long immutableID = immutable.getId();

    // restart flusher
    final Thread flusher2 = new Thread(engine.new Flusher());
    flusherField.set(engine, flusher2);
    flusher2.start();

    // wait for flushing to complete
    assertDoesNotThrow(() -> await().atMost(10, SECONDS).until(() ->
        Util.getDiskArtifact(dataDir, Long.toString(immutableID), ".l2.data").size() == 1 &&
            Util.getDiskArtifact(dataDir, Long.toString(immutableID), ".l2.index").size() == 1 &&
            immutables.peekLast() == null
    ));


    /*
    Now read the index file and data file and create a memtable and compare with immutable.
     */
    final Index l2Index = loadIndexFromDisk(Util.getDiskArtifact(dataDir, Long.toString(immutableID), ".l2.index").get(0));
    final Data l2Data = loadDataFromDisk(Util.getDiskArtifact(dataDir, Long.toString(immutableID), ".l2.data").get(0));
    final List<Pair<byte[], byte[]>> flushedRecords = loadKeyVals(l2Index, l2Data);
    final List<Pair<byte[], byte[]>> memtableRecords = immutable.export();
    flushedRecords.sort((a, b) -> UnsignedBytes.lexicographicalComparator().compare(a.getLeft(), b.getLeft()));
    memtableRecords.sort((a, b) -> UnsignedBytes.lexicographicalComparator().compare(a.getLeft(), b.getLeft()));
    assertTrue(recordsEquals(flushedRecords, memtableRecords));


    testDB.stop().get();
  }


  private List<Pair<byte[], byte[]>> loadKeyVals(final Index i, final Data d) throws IllegalAccessException {
    final DataImpl data = (DataImpl) d;
    final IndexImpl index = (IndexImpl) i;
    final List<Pair<byte[], byte[]>> records = new ArrayList<>();
    final Field indexMapField = getField(index.getClass(), "map", true);
    final Map<ByteArrayWrapper, Long> indexMap = (Map<ByteArrayWrapper, Long>) indexMapField.get(index);
    indexMap.forEach((k, v) -> {
      if (v == 0) {
        records.add(ImmutablePair.of(k.unwrap(), null));
      } else {
        final byte[] val = data.getValue(v);
        records.add(ImmutablePair.of(k.unwrap(), val));
      }
    });
    return records;
  }

  private boolean recordsEquals(final List<Pair<byte[], byte[]>> a, final List<Pair<byte[], byte[]>> b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      if (!Arrays.equals(a.get(i).getLeft(), b.get(i).getLeft()) ||
          !Arrays.equals(a.get(i).getRight(), b.get(i).getRight())) {
        LOG.error("Mismatch at index: {}", i);
        return false;
      }
    }
    return true;
  }
}
