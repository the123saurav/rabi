package com.rabi.internal.db.engine.wal;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Wal;
import com.rabi.internal.exceptions.InvalidCheckpointException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

/*
  TODO: add CRC event.
  Think about multi writes, do we need to worry about
  write to WAL, i don't think so as long as we don't specify offset to write.
  The syscalls are always atomic and we dont need to keep track of offset.

  THINK about using mem outside Heap for GC overhead skipping.

  For parallelising, we can have:
  - multiple WAL segments per memtable which governs parallelism.
    We have either one thread writer per segment and writing work
    to thread via queue per thread.
    We can use completablefuture executor too.
    https://stackoverflow.com/questions/22308158/writing-a-file-using-multiple-threads

  Note that each memtable is backed by one WAL which can have many segments.
  This allows flushing to unlink the WAL after flush.

  Having multiple threads(segments) writing to same underlying disk can be faster if not using sync mode
  as then writes are just write to OS's page cache.
 */
public class WalImpl implements Wal {

  private final long ts;
  private final Segment[] segments;
  private final AtomicLong next;
  private final Logger log;
  private boolean closed = false;

  public WalImpl(long t, Segment[] segs, Logger logger) {
    ts = t;
    next = new AtomicLong();
    segments = segs;
    log = logger;
  }

  @Override
  public List<Record> load() {
    log.debug("loading WAL: " + ts);
    final List<Record> records = new ArrayList<>();
    for (final Segment s : segments) {
      try {
        records.addAll(s.load());
      } catch (IOException e) {
        throw new InitialisationException(e);
      }
    }
    return records;
  }

  @Override
  public long appendPut(final byte[] k, final byte[] v) throws IOException {
        /*
         vTime is used for ordering across segments during load.
         As can be seen the ordering is when we wrote to WAL and not when the operation
         returned to caller.
         */
    final long vTime = next.incrementAndGet();
        /*
        now at this point all concurrent threads have a distinct
        value.
         */
    segments[(int) vTime % segments.length].appendPut(k, v);
    return vTime;
  }

  @Override
  public long appendDelete(byte[] k) throws IOException {
    long vTime = next.incrementAndGet();
        /*
        now at this point all concurrent threads have a distinct
        value.
         */
    segments[(int) vTime % segments.length].appendDelete(k);
    return vTime;
  }

  @Override
  public void checkpoint(long offset) throws InvalidCheckpointException {

  }

  @Override
  public long getCheckpointOffset() {
    return 0;
  }

  @Override
  public long getLastOffset() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      for (Segment segment : segments) {
        segment.close();
      }
      closed = true;
    }
  }

  @Override
  public void renameToTmp() throws IOException {
    for (Segment segment : segments) {
      segment.renameToTmp();
    }
  }

  @Override
  public void unlink() throws IOException {
    for (Segment segment : segments) {
      segment.unlink();
    }
  }
}
