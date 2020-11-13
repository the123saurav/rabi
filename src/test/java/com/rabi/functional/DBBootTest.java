package com.rabi.functional;

import static com.rabi.Util.getAlphaNumericString;
import static com.rabi.Util.loadDataFromDisk;
import static com.rabi.Util.loadIndexFromDisk;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


import com.rabi.Config;
import com.rabi.DBFactory;
import com.rabi.Util;
import com.rabi.exceptions.WritesStalledException;
import com.rabi.internal.db.DBImpl;
import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.EngineImpl;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.db.engine.memtable.MemTableImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import com.rabi.unit.TestConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DBBootTest {

  private static final Logger log = LoggerFactory.getLogger(DBBootTest.class);

  /*
  Here we create WAL, L2, L3 disk artifacts, stop DB, read them directly via filesystem.
  Then we start DB and validate that all are loaded fine and keys put == keys in MT(WAL), IMT, L2 + L3.
   */
  @Test
  void testBoot(@TempDir final Path dataDir) throws InterruptedException, IOException, IllegalAccessException {
    log.info("*********** Starting testBoot **********");
    final DBImpl testDB = (DBImpl) DBFactory.getInstance(dataDir.toString(), log);
    final Config cfg = new Config.ConfigBuilder().setMemtableMaxKeys(100000).build();
    log.info("Config is {}", cfg);
    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, SECONDS);
    });
    log.info("Engine started now in testBoot");

    // Let flusher and compactor start
    final Field engineField = getField(testDB.getClass(), "engine", true);
    final EngineImpl engine = (EngineImpl) engineField.get(testDB);

    final Field compactorField = getField(engine.getClass(), "compactor", true);
    final Thread compactor = (Thread) compactorField.get(engine);

    final Field flusherField = getField(engine.getClass(), "flusher", true);
    final Thread flusher = (Thread) flusherField.get(engine);
    await().atMost(5, SECONDS).until(() ->
        compactor.isAlive() && flusher.isAlive()
    );

    // Put keys in DB
    final List<Pair<byte[], byte[]>> keysPutInDB = new ArrayList<>();
    final long numPuts = 600000;
    for (int j = 0; j < numPuts; j++) {
      try {
        final byte[] k = getAlphaNumericString(4).getBytes();
        testDB.put(k, k);
        keysPutInDB.add(ImmutablePair.of(k, k));
      } catch (final WritesStalledException e) {
        log.warn("writes stalled, waiting...");
        Thread.currentThread().sleep(100);
      }
    }
    final Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<>();
    keysPutInDB.forEach(pair -> map.put(new ByteArrayWrapper(pair.getLeft()), new ByteArrayWrapper(pair.getRight())));
    log.info("Inserted {} unique keys in DB", map.size());
    Thread.currentThread().sleep(5000);

    testDB.stop();
    Thread.currentThread().sleep(1000);

    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, SECONDS);
    });
    log.info("Engine started again in testBoot");

    final Field mutableField = getField(engine.getClass(), "mutableTable", true);
    final MemTableImpl mutableMemTable = (MemTableImpl) mutableField.get(engine);
    final List<Pair<byte[], byte[]>> keysPresentInDB = new ArrayList<>(mutableMemTable.export());

    final Field immutableTablesField = getField(engine.getClass(), "immutableTables", true);
    final ConcurrentLinkedDeque<MemTable> immutableTables = (ConcurrentLinkedDeque<MemTable>) immutableTablesField.get(engine);
    immutableTables.forEach(mt -> keysPresentInDB.addAll(mt.export()));

    final Field l2IndexesField = getField(engine.getClass(), "l2Indexes", true);
    final Map<Long, Index> l2Indexes = (Map<Long, Index>) l2IndexesField.get(engine);
    final Field l2DataField = getField(engine.getClass(), "l2Data", true);
    final Map<Long, Data> l2Data = (Map<Long, Data>) l2DataField.get(engine);
    log.info("Loading l2Data to memory");
    for (Data data : l2Data.values()) {
      data.loadValues();
      log.info("Loaded l2Data for {}", data.getID());
    }
    l2Indexes.values().forEach(i -> {
      try {
        keysPresentInDB.addAll(loadKeyVals(i, l2Data.get(i.getId())));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });

    final Field l3IndexesField = getField(engine.getClass(), "l3Indexes", true);
    final Map<Long, Index> l3Indexes = (Map<Long, Index>) l3IndexesField.get(engine);
    final Field l3DataField = getField(engine.getClass(), "l3Data", true);
    final Map<Long, Data> l3Data = (Map<Long, Data>) l3DataField.get(engine);
    log.info("Loading l3Data to memory");
    for (Data data : l3Data.values()) {
      data.loadValues();
      log.info("Loaded l3Data for {}", data.getID());
    }
    l3Indexes.values().forEach(i -> {
      try {
        keysPresentInDB.addAll(loadKeyVals(i, l3Data.get(i.getId())));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });

    final Map<ByteArrayWrapper, ByteArrayWrapper> map2 = new HashMap<>();
    keysPresentInDB.forEach(pair -> map2.put(new ByteArrayWrapper(pair.getLeft()), new ByteArrayWrapper(pair.getRight())));
    log.info("Unique keys Present In DB: {}", map2.size());

    assertEquals(map.size(), map2.size());
    assertEquals(map, map2);
    testDB.stop();
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

}
