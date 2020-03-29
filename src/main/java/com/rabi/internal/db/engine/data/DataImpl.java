package com.rabi.internal.db.engine.data;

import com.rabi.internal.db.engine.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static com.rabi.internal.db.engine.util.FileUtils.atomicWrite;

/**
 * A data file upon creation has a .tmp extension.
 * We know the size of the file at the beginning.
 * Do we create a sparse file?
 *
 * DataImpl deals with boot
 * Format:
 * <8><8><m><n>
 *
 * It provides interfaces for:
 * - append list of data using fileChannel. we preallocate first so that we fail fast.
 * - respond to get requests using seek.
 * - rename file from .TMP
 */
public class DataImpl implements Data {
    private static final int BUFFER_SIZE_BYTES = 128 * 1024;
    private static final int MAX_ENTRY_SIZE_BYTES = 65794;
    private static final Logger log = LoggerFactory.getLogger(DataImpl.class);
    private Path path;
    private final boolean syncMode;

    public DataImpl(Path p, boolean sm){
        path = p;
        syncMode = sm;
    }

    /**
     *
     * flush operation could be slow as it not in hot path, should not affect GC much.
     * flush checks if we have disk and inodes available so that we don't fail while writing and end in
     * inconsistent state. It creates a batch of entries to be flushed and
     * then preallocates that length.
     * @param entries - list of values to append to file.
     */
    public void flush(List<Pair<byte[], byte[]>> entries) throws IOException {
        //TODO: think about direct buffers
        ByteBuffer b = ByteBuffer.allocate(BUFFER_SIZE_BYTES);

        Set<OpenOption> opts = new HashSet<>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        {
            if (syncMode) {
                opts.add(StandardOpenOption.DSYNC);
            }
        }
        //We are not allocating disk space here, so it grows after 128KB. We can optimise it.
        // RAF.setLength creates sparse file and hence doesn't guarantee disk space.
        try(FileChannel ch = FileChannel.open(path, opts)){
            /*
            To guarantee atomic writes(which OS doesnt provide as no FS is transational),
            we can do 2 things:
            - check free disk space and inodes in partition before every write, again this is not fullproof with small
              race window.
            - actually write 0s to the file(we can use this approach to preallocate too), this is the best guarantee
              as we are actually allocating space.
             */
            for(final Pair<byte[], byte[]> e: entries){
                // data file has only PUT values.
                if(e.getRight() != null) {
                    b.put(new Entry(e.getLeft(), e.getRight()).serialize());
                    if (b.remaining() < MAX_ENTRY_SIZE_BYTES) {
                        b.flip();
                        log.info("writing to data file: {} bytes", b.limit());
                        atomicWrite(ch, b);
                        b.rewind();
                    }
                }
            }
            if(b.position() > 0) {
                b.flip();
                log.info("writing last chunk to data file: {} bytes", b.limit());
                atomicWrite(ch, b);
            }
        }
    }

    public void rename(Path n) throws IOException {
        path = Files.move(path, n);
    }

    public byte[] getValue(long offset){
        return null;
    }
}
