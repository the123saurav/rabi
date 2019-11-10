package com.rabi.internal.db.engine.index;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Header for index file.
 * <p>
 * Reserved 96 bytes at start.
 */
public class Header {

    private static final short VERSION = 0;
    static final short HEADER_LENGTH_BYTES = 97;

    private final long totalKeys;
    private final long minKeyOffset;
    private final long maxKeyOffset;

    private Header(long t, long minoff, long maxOff) {
        totalKeys = t;
        minKeyOffset = minoff;
        maxKeyOffset = maxOff;
    }

    long getMinKeyOffset() {
        return minKeyOffset;
    }

    long getMaxKeyOffset() {
        return maxKeyOffset;
    }

    long getTotalKeys() {
        return totalKeys;
    }

    ByteBuffer serialize() {
        return (ByteBuffer) ByteBuffer.allocate(HEADER_LENGTH_BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(0) //tombstone 8 bytes
                .put((byte) VERSION)
                .putLong(totalKeys)
                .putLong(minKeyOffset)
                .putLong(maxKeyOffset)
                .rewind();
    }

    public static Header tryDeserialize(ByteBuffer b) {
        try {
            return deserialize(b);
        } catch (BufferUnderflowException ex) {
            return null;
        }
    }

    public static Header deserialize(ByteBuffer b) {
        b.position(b.position() + 9);
        long t = b.getLong();
        long minOff = b.getLong();
        long maxOff = b.getLong();
        return new Header(t, minOff, maxOff);
    }
}
