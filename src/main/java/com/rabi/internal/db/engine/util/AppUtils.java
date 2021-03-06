package com.rabi.internal.db.engine.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class AppUtils {

  public static long findByteRange(byte[] a, byte[] b, int numBytes) {
    numBytes = Math.min(7, Math.min(numBytes, Math.min(a.length, b.length)));
    a = Arrays.copyOf(a, numBytes);
    b = Arrays.copyOf(b, numBytes);
    long aVal = getByteArrayValue(a);
    long bVal = getByteArrayValue(b);
    return Math.abs(bVal - aVal);
  }

  private static long getByteArrayValue(byte[] a) {
    int aLen = a.length;
    int gap = 8 - aLen;
    a = Arrays.copyOf(a, 8);
    int i = a.length - 1;
    while (i - gap >= 0) {
      a[i] = a[i - gap];
      i--;
    }
    while (i >= 0) {
      a[i--] = 0; //fill with 0s
    }
    return ByteBuffer.wrap(a).getLong(); // 7 byte array can always be accommodated in long
  }

  public static int compare(final byte[] left, final byte[] right) {
    for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
      int a = (left[i] & 0xff);
      int b = (right[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }
}
