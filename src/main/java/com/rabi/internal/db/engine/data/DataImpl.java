package com.rabi.internal.db.engine.data;

import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.Bootable;
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


import static com.rabi.internal.db.engine.util.FileUtils.safeAppend;

/**
 * We know the size of the file at the beginning.
 * Do we create a sparse file?
 * <p>
 * DataImpl deals with boot
 * Format:
 * <version> (1) - TODO
 * <1><2><m><n>
 * <p>
 * It provides interfaces for:
 * - append list of data using fileChannel. we preallocate first so that we fail fast.
 * - respond to get requests using seek.
 */
public class DataImpl implements Data {
  private static final int BUFFER_SIZE_BYTES = 128 * 1024;
  private static final int MAX_ENTRY_SIZE_BYTES = 65794;
  private static final Logger log = LoggerFactory.getLogger(DataImpl.class);
  private Path path;
  private final long id;
  private long size;
  private Map<Long, byte[]> values;

  public DataImpl(final Path p, final long i, final long sz) {
    path = p;
    id = i;
    size = sz;
  }

  @Override
  public long getID() {
    return id;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public long getSize() {
    return 0;
  }


  /**
   * flush operation could be slow as it not in hot path, but should not affect GC much.
   * It creates a batch of records to be flushed and
   * then preallocates that length.
   *
   * @param records - list of values to append to file.
   */
  public void flush(final List<Pair<byte[], byte[]>> records, boolean syncMode) throws IOException {
    //TODO: think about direct buffers
    final ByteBuffer b = ByteBuffer.allocate(BUFFER_SIZE_BYTES);

    final Set<OpenOption> opts = new HashSet<>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.WRITE));
    {
      if (syncMode) {
        opts.add(StandardOpenOption.DSYNC);
      }
    }
    //We are not allocating disk space here, so it grows after 128KB. We can optimise it.
    // RAF.setLength creates sparse file and hence doesn't guarantee disk space.
    try (final FileChannel ch = FileChannel.open(path, opts)) {
            /*
            To guarantee atomic writes(which OS doesn't provide as no FS is transactional),
            we can do 2 things:
            - check free disk space and inodes in partition before every write, again this is not foolproof with small
              race window.
            - actually write 0s to the file(we can use this approach to preallocate too), this is the best guarantee
              as we are actually allocating space.
             */
      //simulate append
      ch.position(ch.size());
      for (final Pair<byte[], byte[]> r : records) {
        // data file has only PUT values.
        if (r.getRight() != null) {
          b.put(new Record(r.getLeft(), r.getRight()).serialize());
          if (b.remaining() < MAX_ENTRY_SIZE_BYTES) {
            b.flip();
            log.debug("writing to data file: {} bytes", b.limit());
            // TODO: Do we need this given we create .tmp file?
            size += safeAppend(ch, b);
            //b.rewind();
            b.clear();
          }
        }
      }
      if (b.position() > 0) {
        b.flip();
        log.info("writing last chunk to data file: {} bytes", b.limit());
        size += safeAppend(ch, b);
      }
    }
  }

  public void rename(final Path n) throws IOException {
    log.info("Renaming {} to {}", path, n);
    path = Files.move(path, n);
  }

  @Override
  public void unlink() {
    try {
      log.info("Removing data file: {}", path);
      Files.delete(path);
    } catch (IOException e) {
    }
  }

  public byte[] getValue(final long offset) {
    return null;
  }

  @Override
  public void loadValues() throws IOException {
    values = new HashMap<>();
    final FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
    final ByteBuffer loadBuffer = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
    Record e;
    loadBuffer.mark();
    long offset = 0;
    while (ch.read(loadBuffer) > 0) {
      loadBuffer.flip();// trim buffer to filled value
      while (loadBuffer.hasRemaining()) { // while we have not read whole buffer
        loadBuffer.mark();
        e = Record.tryDeserialize(loadBuffer);
        if (e == null) {
          loadBuffer.reset();
          break;
        }
        values.put(offset, e.getVal());
        offset += Record.getDiskSize(e.getKey(), e.getVal());
      }
      loadBuffer.mark();
    }
    log.info("Loaded {} records to memory for data {}", values.size(), path);
  }

  @Override
  public void offloadValues() {
    values = null;
  }

  @Override
  public byte[] getValueForOffset(final long offset) {
    return values.get(offset);
  }

  /*public Iterator<Pair<byte[], byte[]>> iterator() {
    return new Itr();
  }*/

  /*private class Itr implements Iterator<Pair<byte[], byte[]>> {

    private final FileChannel iterChannel;
    private final ByteBuffer buffer;
    private boolean needLoading;

    Itr() {
      try {
        iterChannel = FileChannel.open(path, StandardOpenOption.READ);
        buffer = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
        needLoading = false;
      } catch (IOException e) {
        throw new InitialisationException("Error in loading data file: " + path + e.getMessage(), e);
      }
    }


    private boolean tryLoadRecords() {
      buffer.mark();
      int bytesRead = 0;
      try {
        bytesRead = iterChannel.read(buffer);
      } catch (IOException e) {
        throw new RuntimeException("Error in loading new Data records", e);
      }
      buffer.flip();// trim buffer to filled value freezing its size
      return bytesRead != 0;
    }

    @Override
    public boolean hasNext() {
      if (buffer.hasRemaining()) {
        return true;
      }
      return tryLoadRecords();
    }

    @Override
    public Pair<byte[], byte[]> next() {
      while (buffer.hasRemaining()) { // while we have not read whole buffer
        buffer.mark();
        final Record r = Record.tryDeserialize(buffer);
        if (r == null) {
          buffer.reset(); // set to offset last read as it failed because of partial record read
          tryLoadRecords();
        } else {
          return new ImmutablePair<>(r.getKey(), r.getVal());
        }
      }
      return null;
    }
  }*/

  public static class DataBooter implements Bootable<Data> {
    private final Path p;

    public DataBooter(final Path p) {
      this.p = p;
    }

    @Override
    public Data boot() throws IOException {
      log.info("Booting data file at: {}", p);
      long id = FileUtils.getId(p);
      long sz = Files.size(p);
      return new DataImpl(p, id, sz);
    }
  }
}
