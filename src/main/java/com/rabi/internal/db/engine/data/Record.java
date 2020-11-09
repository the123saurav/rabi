package com.rabi.internal.db.engine.data;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final public class Record {

  /*
  TODO: add a version and a check that file version is same as us hardcoded.
  We support only 1 version (even though the higher version can have lower version parsers)
  as then we would have to use that for all existing artifacts.
  This can be done in future.
   */
  private final byte[] key; //TODO: Can we just have this package private to avoid method call
  private final byte[] val;

  Record(byte[] k, byte[] v) {
    key = k;
    val = v;
    assert key.length > 0;
    assert val.length > 0;
  }

  public static long getDiskSize(final byte[] k, final byte[] v) {
    return 1 + 2 + k.length + v.length;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getVal() {
    return val;
  }

  //TODO: check if we need key in data file as it already in index.

  /**
   * Max size of returned buffer is 1 + 2 + 256 + 65535 = 65794 bytes.
   *
   * @return
   */
  ByteBuffer serialize() {
    return (ByteBuffer) ByteBuffer.allocate(1 + 2 + key.length + val.length)
        .order(ByteOrder.BIG_ENDIAN)
        .put((byte) key.length)
        .putShort((short) val.length)
        .put(key)
        .put(val)
        .rewind();
  }

  static Record tryDeserialize(final ByteBuffer b) {
    try {
      return deserialize(b);
    } catch (final BufferOverflowException ex) {
      /*
        indicates we had a partial record in buffer and hence we need to read more.
       */
      return null;
    }
  }

  private static Record deserialize(final ByteBuffer b) {
    final short keyLen = b.get();
    final short valLen = b.getShort();
    final byte[] k = new byte[keyLen];
    final byte[] v = new byte[valLen];
    b.get(k);
    b.get(v);
    return new Record(k, v);
  }
}
