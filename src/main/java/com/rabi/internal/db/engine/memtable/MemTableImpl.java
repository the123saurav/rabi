package com.rabi.internal.db.engine.memtable;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.MemTable;
import com.rabi.internal.db.engine.Wal;
import com.rabi.internal.db.engine.wal.Entry;
import com.rabi.internal.types.ByteArrayWrapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

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
            if (other == this) return true;
            if (!(other instanceof Value)) return false;
            return Arrays.equals(val, ((Value) other).val);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(val);
        }
    }

    private ConcurrentHashMap<ByteArrayWrapper, Value> m;
    private Wal wal;
    private boolean mutable;
    private Logger log;

    private static BiFunction<Value, Value, Value>
            mapper = (purana, naya) -> purana.vTime > naya.vTime ? purana : naya;

    public MemTableImpl(Wal w, Logger logger) {
        m = new ConcurrentHashMap<>();
        wal = w;
        log = logger;
    }

    public void load() {
        List<Entry> entries = null;
        try {
            entries = wal.load();
        } catch (IOException e) {
            throw new InitialisationException(e);
        }
        //now order entries as per vtime.
        entries.sort(null);

        for (Entry e : entries) {
            byte[] key = e.getKey();
            long vTime = e.getVTime();
            if (e.getOp() == Entry.OpType.DELETE) {
                /*
                    since we are processing in order, we don't need to
                    compare vTime
                 */
                m.put(new ByteArrayWrapper(key), new Value(null, vTime));
            } else {
                m.put(new ByteArrayWrapper(key), new Value(e.getVal(), vTime));
            }
        }
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
    }

    public void allowMutation() {
        mutable = true;
    }

    public void disallowMutation() {
        mutable = false;
    }

    @Override
    public void put(byte[] k, byte[] v) throws IOException {
        long vtime = wal.appendPut(k, v);
        ByteArrayWrapper b = new ByteArrayWrapper(k);
        put(b, v, vtime);
    }

    @Override
    public void delete(byte[] k) throws IOException {
        long vtime = wal.appendDelete(k);
        ByteArrayWrapper b = new ByteArrayWrapper(k);
        put(b, null, vtime);
    }

    // this is not lock free as it internally locks the section.
    private void put(ByteArrayWrapper b, byte[] v, long vtime) {
        m.merge(b, new Value(v, vtime), mapper);
        /*Value curr = m.get(b);
        if(curr == null || vtime > curr.vTime){
            m.put(b, new Value(v, vtime));
        }*/
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}


