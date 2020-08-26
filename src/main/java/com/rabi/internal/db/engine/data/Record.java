package com.rabi.internal.db.engine.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class Record {

    /*
    TODO: add a version and a check that file version is same as us hardcoded.
    We support only 1 version (even though the higher version can have lower version parsers)
    as then we would have to use that for all existing artifacts.
    This can be done in future.
     */
    private final byte[] key;
    private final byte[] val;

    Record(byte[] k, byte []v){
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
