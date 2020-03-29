package com.rabi.internal.db.engine.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Entry {

    private final byte[] key;
    private final byte[] val;

    Entry(byte[] k, byte []v){
        key = k;
        val = v;
    }

    //TODO: check if we need key in data file as it already in index.

    /**
     * Max size of returned buffer is 1 + 2 + 256 + 65535 = 65794 bytes.
     * @return
     */
    ByteBuffer serialize(){
        return (ByteBuffer) ByteBuffer.allocate(1 + 2 + key.length + val.length)
                .order(ByteOrder.BIG_ENDIAN)
                .put((byte) key.length)
                .putShort((short)val.length)
                .put(key)
                .put(val)
                .rewind();
    }
}
