package com.rabi.functional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.rabi.Config;
import com.rabi.DBFactory;
import com.rabi.Util;
import com.rabi.internal.db.DBImpl;
import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.EngineImpl;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.util.FileUtils;
import com.rabi.internal.types.ByteArrayWrapper;
import com.rabi.unit.TestConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CompactionTest {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionTest.class);

  /*
  Q. What do we test here:
  A. We test all code paths for regular compaction non-subset case.
  For this we create l2 and l3 artifacts.
  For L3, we create say 3 l3 files.
  For L2, we create 1 l2 file for non-subset case.
  Now the highest density L2 file should be chosen.
  For the candidate L2 file, it should have following case:

   L2 ->     |-----------------------------------------------------------------------|
   L3 ->            |--------|       |---------|         |---------|    |---------|
                        L31   <--a-->    L32    <---b--->    L33    <-c->   L34

   Note that orphan keys in L2 before L31, b/n  'a' and 'b'  would create new L3 indices.
   But keys in 'c' would be distributed among L33 and L34 and keys at end of L2 would be merged to L34.

   To validate that L2 data has been moved rightly, we will iterate over L2 index keys and lookup in the
   expected L3 index and data file(we identify L3 artifacts via their range, as we would know the new range of each L3).
   For validation, we would explicitly load l3 data and index file.
   */
  @Test
  void testRegularCompactionNonSubsetCase(@TempDir final Path dataDir) throws IOException, IllegalAccessException, InterruptedException {
    LOG.info("\n\n**** testRegularCompactionNonSubsetCase *******\n\n");
    final List<Pair<byte[], byte[]>> l31Records = new ArrayList<>();
    List<byte[]> keys = generateKeysInRange("ccc".getBytes(), "ddd".getBytes(), 3);
    keys.forEach(k -> l31Records.add(ImmutablePair.of(k, k)));
    FileUtils.flushLevelFiles(l31Records, dataDir, 31, true, "l3");

    final List<Pair<byte[], byte[]>> l32Records = new ArrayList<>();
    keys = generateKeysInRange("fff".getBytes(), "ggg".getBytes(), 3);
    keys.forEach(k -> l32Records.add(ImmutablePair.of(k, k)));
    FileUtils.flushLevelFiles(l32Records, dataDir, 32, true, "l3");

    final List<Pair<byte[], byte[]>> l33Records = new ArrayList<>();
    keys = generateKeysInRange("iii".getBytes(), "jjj".getBytes(), 3);
    keys.forEach(k -> l33Records.add(ImmutablePair.of(k, k)));
    FileUtils.flushLevelFiles(l33Records, dataDir, 33, true, "l3");

    final List<Pair<byte[], byte[]>> l34Records = new ArrayList<>();
    keys = generateKeysInRange("lll".getBytes(), "mmm".getBytes(), 3);
    keys.forEach(k -> l34Records.add(ImmutablePair.of(k, k)));
    FileUtils.flushLevelFiles(l34Records, dataDir, 34, true, "l3");

    final List<byte[]> l2Keys = Stream.of(
        "aaa", "bbb",
        "ccd", "cot",
        "eee", "egg", "eye",
        "fgg", "fgh", "fgz",
        "hhh", "his", "her",
        "iit", "ink",
        "kkk",
        "lom", "log",
        "not"
    ).map(String::getBytes).collect(Collectors.toList());
    final List<Pair<byte[], byte[]>> l21Records = new ArrayList<>();
    l2Keys.forEach(k -> l21Records.add(ImmutablePair.of(k, k)));
    FileUtils.flushLevelFiles(l21Records, dataDir, 21, true, "l2");

    final int MEMTABLE_MAX_KEYS = 1000;

    final DBImpl testDB = (DBImpl) DBFactory.getInstance(dataDir.toString(), LOG);

    final Config cfg = new Config.ConfigBuilder().setMemtableMaxKeys(MEMTABLE_MAX_KEYS).build();
    final Field minOrphanedKeysDuringCompactionField = getField(Config.class,
        "minOrphanedKeysDuringCompaction", true);
    minOrphanedKeysDuringCompactionField.setInt(cfg, 1);
    final Field maxFlushedFilesField = getField(Config.class,
        "maxFlushedFiles", true);
    maxFlushedFilesField.setInt(cfg, 0);

    assertDoesNotThrow(() -> {
      final CompletableFuture<Void> isOpen = testDB.open(cfg);
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, TimeUnit.SECONDS);
    });
    LOG.info("Engine started now in testRegularCompactionNonSubsetCase");

    /*
    Now compaction would be auto-triggered at start.
    We wait for sometime and then test that required disk artifacts are created.
    There would be 7 l3 artifacts.
    We read them and arrange them via sorted order of ranges.
     */
    assertDoesNotThrow(() ->
        await().atMost(10, SECONDS).until(() ->
            Util.getDiskArtifact(dataDir, ".l3.data").size() == 7 &&
                Util.getDiskArtifact(dataDir, ".l3.index").size() == 7
        ));

    final Field engineField = getField(testDB.getClass(), "engine", true);
    final EngineImpl engine = (EngineImpl) engineField.get(testDB);
    final Field l2IndexesField = getField(engine.getClass(), "l2Indexes", true);
    final Map<Long, Index> l2Indexes = (Map<Long, Index>) l2IndexesField.get(engine);
    final Field l2DataField = getField(engine.getClass(), "l2Data", true);
    final Map<Long, Data> l2Data = (Map<Long, Data>) l2DataField.get(engine);


    Thread.currentThread().sleep(2000);
    final List<Path> l3IndexPaths = Util.getDiskArtifact(dataDir, ".l3.index");
    final List<Path> l3DataPaths = Util.getDiskArtifact(dataDir, ".l3.data");
    final List<Index> l3Indexes = l3IndexPaths.stream().map(Util::loadIndexFromDisk).collect(Collectors.toList());
    l3Indexes.sort(Comparator.comparing(a -> new ByteArrayWrapper(a.getMinKey())));
    l3Indexes.forEach(l3 -> {
      LOG.info("Printing keys in index {}", l3.getId());
      l3.getKeys().forEach(k -> LOG.info(new String(k.unwrap())));
    });

    Map<Long, Data> l3IdToData = new HashMap<>();
    for (final Path l3DataPath : l3DataPaths) {
      final Data data = Util.loadDataFromDisk(l3DataPath);
      l3IdToData.put(data.getID(), data);
    }

    assertEquals("aaa", new String(l3Indexes.get(0).getMinKey()));
    assertEquals("bbb", new String(l3Indexes.get(0).getMaxKey()));
    assertEquals(2, l3Indexes.get(0).getTotalKeys());
    assertEquals(2, l3Indexes.get(0).getKeys().size());

    assertEquals("ccc", new String(l3Indexes.get(1).getMinKey()));
    assertEquals("ddd", new String(l3Indexes.get(1).getMaxKey()));
    assertEquals(5, l3Indexes.get(1).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("ccc", "ccc1", "ddd", "ccd", "cot")),
        new HashSet<>(l3Indexes.get(1).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals("eee", new String(l3Indexes.get(2).getMinKey()));
    assertEquals("eye", new String(l3Indexes.get(2).getMaxKey()));
    assertEquals(3, l3Indexes.get(2).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("eye", "eee", "egg")),
        new HashSet<>(l3Indexes.get(2).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals("fff", new String(l3Indexes.get(3).getMinKey()));
    assertEquals("ggg", new String(l3Indexes.get(3).getMaxKey()));
    assertEquals(6, l3Indexes.get(3).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("fff1", "ggg", "fgh", "fff", "fgg", "fgz")),
        new HashSet<>(l3Indexes.get(3).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals("her", new String(l3Indexes.get(4).getMinKey()));
    assertEquals("his", new String(l3Indexes.get(4).getMaxKey()));
    assertEquals(3, l3Indexes.get(4).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("his", "hhh", "her")),
        new HashSet<>(l3Indexes.get(4).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals("iii", new String(l3Indexes.get(5).getMinKey()));
    assertEquals("kkk", new String(l3Indexes.get(5).getMaxKey()));
    assertEquals(6, l3Indexes.get(5).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("iit", "iii1", "ink", "jjj", "kkk", "iii")),
        new HashSet<>(l3Indexes.get(5).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals("lll", new String(l3Indexes.get(6).getMinKey()));
    assertEquals("not", new String(l3Indexes.get(6).getMaxKey()));
    assertEquals(6, l3Indexes.get(6).getTotalKeys());
    assertEquals(new HashSet<>(Arrays.asList("not", "log", "lll1", "lll", "lom", "mmm")),
        new HashSet<>(l3Indexes.get(6).getKeys().stream().map(
            b -> new String(b.unwrap())).collect(Collectors.toList())));

    assertEquals(0, l2Indexes.size());
    assertEquals(0, l2Data.size());
    testDB.stop();
  }

  /*
      We dom't need the keys to be lexicographically sorted, only range should be not changed.
   */
  private List<byte[]> generateKeysInRange(final byte[] start, final byte[] end, final int numKeys) {
    final List<byte[]> keys = new ArrayList<>();
    keys.add(start);
    for (int i = 1; i < numKeys - 1; i++) {
      keys.add(concatenateByteArrays(start, Integer.toString(i).getBytes()));
    }
    keys.add(end);
    return keys;
  }

  private byte[] concatenateByteArrays(final byte[] a, final byte[] b) {
    byte[] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
}
