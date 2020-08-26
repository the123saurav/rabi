package com.rabi.internal.db.engine.flusher;

import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.Flusher;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.channel.Message;
import com.rabi.internal.db.engine.channel.message.EngineToFlusher;
import com.rabi.internal.db.engine.channel.message.FlusherToEngine;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gets the from and reply channel from engine.
 */
public final class FlusherImpl implements Flusher, Runnable {
  private static final Logger log = LoggerFactory.getLogger(FlusherImpl.class);
  private final BlockingQueue<EngineToFlusher> fromEngine;
  private final BlockingQueue<Message> toEngine;

  public FlusherImpl(BlockingQueue<EngineToFlusher> fe, BlockingQueue<Message> te) {
    fromEngine = fe;
    toEngine = te;
  }


  @Override
  public void run() {
    EngineToFlusher msg;
    while (true) {
      try {
        msg = fromEngine.take(); //block for task
        // engine adds the index and removes immutable table.
        final Index i = doFlush(msg.getMemTable(), msg.getDataDir(), msg.getSyncMode());
        toEngine.add(new FlusherToEngine(i));
      } catch (InterruptedException e) {
        log.info("Shutting down routine...");
        return;
      }
    }
  }

  //TODO: add annotation for expo backoff
  private Index doFlush(final MemTable m, final Path dataDir, final boolean syncMode)
      throws InterruptedException {
    while (true) {
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
  private Index flush(final MemTable m, final Path dataDir, final boolean syncMode)
      throws IOException {
    final long id = m.getId();
    final List<Pair<byte[], byte[]>> recordSet = m.export();
    log.info(
        String.format("Flushing memtable: %s with %d records, id: %d", m.getId(), m.size(), id));
    final Data d = flushDataFile(recordSet, dataDir, id, syncMode);
    final Index i = flushIndexFile(recordSet, dataDir, id, syncMode);
    d.rename(Paths.get(dataDir.toString() + "/" + id + ".l2.data"));
    i.rename(Paths.get(dataDir.toString() + "/" + id + ".l2.index"));
    return i;
  }

  private Data flushDataFile(final List<Pair<byte[], byte[]>> recordSet,
                             final Path dataDir, final long id, final boolean syncMode)
      throws IOException {
    final Data d =
        new DataImpl(Paths.get(dataDir.toString() + "/" + id + ".l2.data.tmp"), syncMode);
    log.info(String.format("Flushing datafile %s", id));
    d.flush(recordSet);
    log.info(String.format("Flushed datafile %s", id));
    return d;
  }

  private Index flushIndexFile(final List<Pair<byte[], byte[]>> entries, Path dataDir, long id,
                               boolean syncMode) throws IOException {
    byte[] tmp = new byte[256];
    Arrays.fill(tmp, (byte) 255);
    ByteArrayWrapper minKey = new ByteArrayWrapper(tmp);
    ByteArrayWrapper maxKey = new ByteArrayWrapper(new byte[] {(byte) 0});
    long minKeyOffset = 0;
    long maxKeyOffset = 0;
    Map<ByteArrayWrapper, Long> m = new HashMap<>();
    ByteArrayWrapper k;
    long fileOffset = 0;
    long currOffset;

    for (final Pair<byte[], byte[]> e : entries) {
      k = new ByteArrayWrapper(e.getLeft());
      currOffset = 0;
      if (e.getRight() != null) {
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

    final Index i =
        IndexImpl.loadedIndex(m, minKey.unwrap(), minKeyOffset, maxKey.unwrap(), maxKeyOffset);
    i.overwrite(Paths.get(dataDir.toString() + "/" + id + ".l2.index.tmp"), syncMode);
    return i;
  }
}
