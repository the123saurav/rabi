package com.rabi.internal.db.engine.index;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.types.ByteArrayWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * The file looks like:
 * <p>
 * 8 bytes free at start.
 * <num_keys> (8)
 * <min_key_offset> (8)
 * <max_key_offset> (8)
 *
 * <key_len><key><offset>
 * 1  +   m +  8
 * <p>
 * Deleted keys are also present in index with offset as 0.
 */
public class IndexImpl implements Index {

    //TODO: use filesystem page size if greater
    private static final int LOAD_SIZE_BYTES = 128 * 1024;

    private final Path path;
    private final Map<ByteArrayWrapper, Long> m;
    private byte[] minKey;
    private long minKeyOffset;
    private byte[] maxKey;
    private long maxKeyOffset;

    public IndexImpl(Path p) {
        path = p;
        m = new HashMap<>();
    }

    private void loadHeader(FileChannel ch) throws IOException {
        ByteBuffer loadBuffer = ByteBuffer.allocateDirect(Header.HEADER_LENGTH_BYTES);
        ch.read(loadBuffer); //maybe empty
        loadBuffer.reset();
        Header h = Header.deserialize(loadBuffer);
        minKeyOffset = h.getMinKeyOffset();
        maxKeyOffset = h.getMaxKeyOffset();
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
                m.put(new ByteArrayWrapper(e.key), e.offset);
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
            loadEntries(ch);
        } catch (IOException e) {
            throw new InitialisationException("Error in loading index file: " + path + e.getMessage(), e);
        }
    }

    @Override
    public void put(byte[] key, long offset) {

    }

    @Override
    public long get(byte[] k) {
        Long l = m.get(k);
        return l == null ? -1 : l;
    }
}
