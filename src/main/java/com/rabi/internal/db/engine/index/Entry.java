package com.rabi.internal.db.engine.index;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Entry {
    final byte[] key;
    final long offset;

    Entry(byte[] k, long o) {
        key = k;
        offset = o;
    }

    ByteBuffer serialize() {
        return (ByteBuffer) ByteBuffer.allocate(1 + key.length + 8)
                .order(ByteOrder.BIG_ENDIAN)
                .put((byte) key.length)
                .put(key)
                .putLong(offset)
                .rewind();
    }

    public static Entry tryDeserialize(ByteBuffer b) {
        try {
            return deserialize(b);
        } catch (BufferOverflowException ex) {
            return null;
        }
    }

    public static Entry deserialize(ByteBuffer b) {
        short t = b.get();
        byte[] k = new byte[t];
        b.get(k);
        long off = b.getLong();
        return new Entry(k, off);
    }
}
