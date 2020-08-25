package com.rabi.internal.db.engine.wal;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Denotes a Record in WAL
 * <p>
 * Has records for Put and Delete operations
 * key_len max 2^8 - 1 bytes
 * val_len max 2^20 -1 bytes
 *
 * <vTime><op_type><key_len><key>[<val_len><val>](Only for PUT)
 * 1+8+1+1+m+4+n
 */
public class Record implements Comparable<Record> {

  private static final short VERSION = 0;

  // Can this be moved over if its public?
  public enum OpType {
    PUT,
    DELETE;

    static OpType get(byte b) {
      return OpType.values()[b];
    }
  }

  private final byte[] key;
  private final byte[] val;
  private final long vTime;
  private final OpType op;

  Record(final byte[] k, final long t) {
    key = k;
    vTime = t;
    op = OpType.DELETE;
    val = null;
  }

  Record(byte[] k, byte[] v, long t) {
    key = k;
    val = v;
    vTime = t;
    op = OpType.PUT;
  }

  public long getVTime() {
    return vTime;
  }

  /**
   * Although we are returning mutable ref,
   * but its used internally by MemtableImpl only.
   *
   * @return
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * Although we are returning mutable ref,
   * but its used internally by MemtableImpl only.
   *
   * @return
   */
  public byte[] getVal() {
    return val;
  }

  public OpType getOp() {
    return op;
  }

  /**
   * Note this returns 0 too which violates
   * the .equals inherited that is because the ordering
   * is explicitly defined and should take precedence.
   *
   * @param o - the record to compare
   * @return {-1,0,1} indicating lt, eq, gt
   */
  @Override
  public int compareTo(final Record o) {
    if (vTime < o.vTime) {
      return -1;
    } else if (vTime == o.vTime) {
      return 0;
    }
    return 1;
  }

  ByteBuffer serialize() {
    if (op == OpType.PUT) {
      return (ByteBuffer) ByteBuffer.allocate(1 + 8 + 1 + 1 + key.length + 4 + val.length)
          .order(ByteOrder.BIG_ENDIAN)
          .put((byte) VERSION)
          .putLong(vTime)
          .put((byte) op.ordinal())
          .put((byte) key.length)
          .put(key)
          .putInt(val.length)
          .put(val).rewind();
    }
    return (ByteBuffer) ByteBuffer.allocate(1 + 8 + 1 + 1 + key.length)
        .put((byte) VERSION)
        .putLong(vTime)
        .put((byte) op.ordinal())
        .put((byte) key.length)
        .put(key).rewind();
  }

  static Record tryDeserialize(ByteBuffer b) {
    try {
      return deserialize(b);
    } catch (BufferUnderflowException ex) {
      return null;
    }
  }

  static Record deserialize(ByteBuffer b) {
    b.position(b.position() + 1);
    long vt = b.getLong();
    OpType o = OpType.get(b.get());
    short kl = b.get();
    byte[] k = new byte[kl];
    b.get(k);
    if (o == OpType.DELETE) {
      return new Record(k, vt);
    }
    int vl = b.getInt();
    byte[] v = new byte[vl];
    b.get(v);

    return new Record(k, v, vt);
  }
}
