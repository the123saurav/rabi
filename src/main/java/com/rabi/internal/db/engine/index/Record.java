package com.rabi.internal.db.engine.index;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class Record {
  final byte[] key;
  final long offset;

  Record(byte[] k, long o) {
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

  static Record tryDeserialize(final ByteBuffer b) {
    try {
      return deserialize(b);
    } catch (BufferUnderflowException ex) {
      return null;
    }
  }

  private static Record deserialize(final ByteBuffer b) {
    final short t = b.get();
    final byte[] k = new byte[t];
    b.get(k);
    final long off = b.getLong();
    return new Record(k, off);
  }
}
