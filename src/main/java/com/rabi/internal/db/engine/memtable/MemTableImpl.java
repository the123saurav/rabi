package com.rabi.internal.db.engine.memtable;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.Wal;
import com.rabi.internal.db.engine.wal.Record;
import com.rabi.internal.types.ByteArrayWrapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

//use jcstress for test
public class MemTableImpl implements MemTable {

  private static class Value {
    final byte[] val;
    final long vTime;

    Value(byte[] v, long vt) {
      val = v;
      vTime = vt;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof Value)) {
        return false;
      }
      return Arrays.equals(val, ((Value) other).val);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(val);
    }
  }

  private final Map<ByteArrayWrapper, Value> m;
  private final Wal wal;
  private final long id;
  private boolean mutable;
  private final Logger log;
  //TODO this is bad, replace with something else even we can sample memory usage
  // with costly call say after every 100 ops. Use HyperLogLog.
  private final AtomicLong numOps;

  private static BiFunction<Value, Value, Value>
      mapper = (purana, naya) -> purana.vTime > naya.vTime ? purana : naya;

  public MemTableImpl(Wal w, long ts, Logger logger) {
    m = new ConcurrentHashMap<>();
    id = ts;
    wal = w;
    log = logger;
    numOps = new AtomicLong();
  }

  public Void boot() {

    List<Record> records;
    try {
      records = wal.load();
    } catch (final IOException e) {
      throw new InitialisationException(e);
    }

    //now order records as per vtime.
    records.sort(null);
    log.info("Memtable {} has {} records in WAL", id, records.size());

    for (final Record e : records) {

      final byte[] key = e.getKey();
      final long vTime = e.getVTime();

      if (e.getOp() == Record.OpType.DELETE) {
                /*
                    since we are processing in order, we don't need to
                    compare vTime
                 */
        if (m.put(new ByteArrayWrapper(key), new Value(null, vTime)) == null) {
          numOps.incrementAndGet();
        }
      } else {
        if (m.put(new ByteArrayWrapper(key), new Value(e.getVal(), vTime)) == null) {
          numOps.incrementAndGet();
        }
      }
    }

    log.info("Memtable {} has {} records in Map", id, m.size());
        /*Iterator<ByteArrayWrapper> it = m.keySet().iterator();
        minKey = it.next();
        maxKey = minKey;

        ByteArrayWrapper k;
        //using below for performance.
        try{
            while(true) {
                k = it.next();
                if (k.compareTo(minKey) < 0) minKey = k;
                if (k.compareTo(maxKey) > 0) maxKey = k;
            }
        }catch (NoSuchElementException ex){
        }*/
    return null;
  }

  public void allowMutation() {
    mutable = true;
  }

  public void disallowMutation() {
    mutable = false;
  }

  @Override
  public long size() {
    return numOps.get();
  }

  @Override
  public void put(final byte[] k, final byte[] v) throws IOException {
    final long vtime = wal.appendPut(k, v);
    final ByteArrayWrapper b = new ByteArrayWrapper(k);
    put(b, v, vtime);
  }

  @Override
  public void delete(final byte[] k) throws IOException {
    final long vtime = wal.appendDelete(k);
    final ByteArrayWrapper b = new ByteArrayWrapper(k);
    put(b, null, vtime);
  }

  // this is not lock free as it internally locks the section.
  private void put(final ByteArrayWrapper b, final byte[] v, final long vtime) {
    // the mapper ensures that the order dictated by WAL's vTime is honoured in map too
    m.merge(b, new Value(v, vtime), mapper);
    numOps.incrementAndGet();
        /*Value curr = m.get(b);
        if(curr == null || vtime > curr.vTime){
            m.put(b, new Value(v, vtime));
        }*/
  }

  @Override
  public void close() throws IOException {
    wal.close();
  }

  @Override
  public List<Pair<byte[], byte[]>> export() {
    return m.entrySet().stream()
        .map(e -> new ImmutablePair<>(e.getKey().unwrap(), e.getValue().val))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  public long getId() {
    return id;
  }

  /**
   * rename wal to .tmp and then unlink.
   */
  @Override
  public void cleanup() throws IOException {
    wal.close();
    wal.renameToTmp(); //so that if below fails, its cleaned up on boot
    wal.unlink();
  }
}


