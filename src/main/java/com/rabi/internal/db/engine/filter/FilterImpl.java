package com.rabi.internal.db.engine.filter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.primitives.UnsignedBytes;
import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class FilterImpl implements Filter {

    private static final int LOAD_SIZE_BYTES = 128 * 1024;
    private static final long MAX_KEYS = 10000000;

    private final Path path;
    private BloomFilter<byte[]> f;
    private byte[] minKey;
    private long minKeyOffset;
    private byte[] maxKey;
    private long maxKeyOffset;
    private long numKeys;

    //TODO a way to tune below params and maybe have it user config driven
    public FilterImpl(Path p) {
        path = p;
    }

    private void loadHeader(FileChannel ch) throws IOException {
        ByteBuffer loadBuffer = ByteBuffer.allocateDirect(Header.HEADER_LENGTH_BYTES);
        ch.read(loadBuffer); //maybe empty
        Header h = Header.deserialize(loadBuffer);
        minKeyOffset = h.getMinKeyOffset();
        maxKeyOffset = h.getMaxKeyOffset();
        numKeys = h.getTotalKeys();
    }

    private void loadEntries(FileChannel ch) throws IOException {
        ByteBuffer loadBuffer = ByteBuffer.allocateDirect(LOAD_SIZE_BYTES);
        Entry e;
        loadBuffer.mark();
        while (ch.read(loadBuffer) > 0) {
            loadBuffer.reset();
            while (loadBuffer.hasRemaining()) {
                loadBuffer.mark();
                e = Entry.tryDeserialize(loadBuffer);
                if (e == null) {
                    loadBuffer.reset();
                    break;
                }
                f.put(e.key);
                if (e.offset == minKeyOffset) {
                    minKey = e.key;
                }
                if (e.offset == maxKeyOffset) {
                    maxKey = e.key;
                }
            }
            loadBuffer.mark();
        }
    }

    @Override
    public void load() {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            loadHeader(ch);
            f = BloomFilter.create(
                    Funnels.byteArrayFunnel(), numKeys, 0.01);
            loadEntries(ch);
        } catch (IOException e) {
            throw new InitialisationException("Error in loading index file: " + path + e.getMessage(), e);
        }
    }

    @Override
    public boolean mayBePresent(byte[] k) {
        if (Arrays.equals(minKey, k) || Arrays.equals(maxKey, k)) {
            return true;
        }
        if (UnsignedBytes.lexicographicalComparator().compare(k, minKey) < 0 ||
                UnsignedBytes.lexicographicalComparator().compare(k, maxKey) > 0
        ) {
            return false;
        }
        return f.mightContain(k);
    }
}
