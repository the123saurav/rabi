package com.rabi.internal.db.engine.data;

import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.Bootable;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.db.engine.util.FileUtils;
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
 * <version> (1)
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
    private final long id;

    public DataImpl(final Path p, final long i){
        path = p;
        id = i;
    }

    @Override
    public long getID() {
        return id;
    }

    @Override
    public Path getPath() {
        return path;
    }

    /**
     *
     * flush operation could be slow as it not in hot path, but should not affect GC much.
     * It creates a batch of records to be flushed and
     * then preallocates that length.
     * 
     * @param records - list of values to append to file.
     */
    public void flush(final List<Pair<byte[], byte[]>> records, boolean syncMode) throws IOException {
        //TODO: think about direct buffers
        final ByteBuffer b = ByteBuffer.allocate(BUFFER_SIZE_BYTES);

        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        {
            if (syncMode) {
                opts.add(StandardOpenOption.DSYNC);
            }
        }
        //We are not allocating disk space here, so it grows after 128KB. We can optimise it.
        // RAF.setLength creates sparse file and hence doesn't guarantee disk space.
        try(final FileChannel ch = FileChannel.open(path, opts)){
            /*
            To guarantee atomic writes(which OS doesn't provide as no FS is transactional),
            we can do 2 things:
            - check free disk space and inodes in partition before every write, again this is not foolproof with small
              race window.
            - actually write 0s to the file(we can use this approach to preallocate too), this is the best guarantee
              as we are actually allocating space.
             */
            for(final Pair<byte[], byte[]> r: records){
                // data file has only PUT values.
                if(r.getRight() != null) {
                    b.put(new Record(r.getLeft(), r.getRight()).serialize());
                    if (b.remaining() < MAX_ENTRY_SIZE_BYTES) {
                        b.flip();
                        log.info("writing to data file: {} bytes", b.limit());
                        // TODO: Do we need this given we create .tmp file?
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

    public void rename(final Path n) throws IOException {
        log.info("Renaming {} to {}", path, n);
        path = Files.move(path, n);
    }

    public byte[] getValue(long offset){
        return null;
    }

    public static class DataBooter implements Bootable<Data> {
        private final Path p;

        public DataBooter(final Path p) {
            this.p = p;
        }

        @Override
        public Data boot() {
            log.info("Booting data file at: {}", p);
            long id = FileUtils.getId(p);
            final Data d = new DataImpl(p, id);
            return d;
        }
    }
}
