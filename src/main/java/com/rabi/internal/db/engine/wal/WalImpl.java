package com.rabi.internal.db.engine.wal;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Wal;
import com.rabi.internal.exceptions.InvalidCheckpointException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/*
  TODO: add CRC event.
  Think about multi writes, do we need to worry about
  write to WAL, i dont think so as long as we dont specify offset to write.
  The syscalls are always atomic and we dont need to keep track of offset.

  THINK about using mem outside Heap for GC overhead skipping.

  For parallelizing, we can have:
  - multiple WAL segments per memtable which governs parallelism.
    We have either one thread writer per segment and writing work
    to thread via queue per thread.
    We can use completablefuture executor too.
    https://stackoverflow.com/questions/22308158/writing-a-file-using-multiple-threads

  Note that each memtable is backed by one WAL which can have many segments.
  This allows flushing to unlink the WAL after flush.
 */
public class WalImpl implements Wal {

    private final long ts;
    private final Segment[] segments;
    private final AtomicLong next;
    private final Logger log;

    public WalImpl(long t, Segment[] segs, Logger logger) {
        ts = t;
        next = new AtomicLong();
        segments = segs;
        log = logger;
    }

    @Override
    public List<Entry> load() {
        log.debug("loading WAL: " + ts);
        List<Entry> entries = new ArrayList<>();
        for (Segment s : segments) {
            try {
                entries.addAll(s.load());
            } catch (IOException e) {
                throw new InitialisationException(e);
            }
        }
        return entries;
    }

    @Override
    public long appendPut(byte[] k, byte[] v) throws IOException {
        long vTime = next.incrementAndGet();
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
        for (Segment segment : segments) {
            segment.close();
        }
    }
}
