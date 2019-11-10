package com.rabi.internal.types;

import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;

public final class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
    private final byte[] data;

    /**
     * Constructor.
     *
     * @param d byte array to be wrapped.
     */
    public ByteArrayWrapper(byte[] d) {
        //TODO: data is an externally mutable field, maybe we should copy it first.
        if (d == null) {
            throw new NullPointerException();
        }
        this.data = d;
    }

    public int length() {
        return data.length;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper) other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(ByteArrayWrapper o) {
        return UnsignedBytes.lexicographicalComparator().compare(data, o.data);
    }
}