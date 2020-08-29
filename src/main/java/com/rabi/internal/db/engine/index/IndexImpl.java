package com.rabi.internal.db.engine.index;

import com.rabi.exceptions.InitialisationException;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.Bootable;
import com.rabi.internal.db.engine.util.AppUtils;
import com.rabi.internal.db.engine.util.FileUtils;
import com.rabi.internal.types.ByteArrayWrapper;
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
import java.util.concurrent.locks.ReentrantLock;


import static com.rabi.internal.db.engine.util.FileUtils.atomicWrite;

/**
 * The file looks like:
 * <p>
 * 8 bytes free at start.
 * <version> (1)
 * <num_keys> (8)
 * <min_key_offset> (8)
 * <max_key_offset> (8)
 *
 * <key_len><key><offset>
 * 1  +       m +   8
 * </p>
 * Deleted keys are also present in index with offset as 0.
 */
public class IndexImpl implements Index {

  //TODO: use filesystem page size if greater
  private static final int BUFFER_SIZE_BYTES = 128 * 1024;
  private static final int MAX_ENTRY_SIZE_BYTES = 265;
  private final static Logger log = LoggerFactory.getLogger(IndexImpl.class);

  private Path path;
  private final long id;
  private volatile boolean shouldLock; // this lock is turned on by comapctor
  private final ReentrantLock lock;
  private Map<ByteArrayWrapper, Long> map;
  private byte[] minKey;
  private long minKeyOffset;
  private byte[] maxKey;
  private long maxKeyOffset;
  private long totalKeys;

  private IndexImpl(final Path p, final long i) {
    path = p;
    id = i;
    map = new HashMap<>();
    lock = new ReentrantLock();
  }

  /**
   * An empty index needs to be loaded from FS.
   *
   * @return
   */
  public static IndexImpl emptyIndex(final Path p, final long id) {
    return new IndexImpl(p, id);
  }

  public static IndexImpl loadedIndex(
      final Path p,
      final long id,
      final Map<ByteArrayWrapper, Long> m,
      final byte[] minKey,
      final long minKeyOffset,
      final byte[] maxKey,
      final long maxKeyOffset) {
    final IndexImpl i = emptyIndex(p, id);
    i.map = m;
    i.minKeyOffset = minKeyOffset;
    i.maxKeyOffset = maxKeyOffset;
    i.totalKeys = m.size(); //check this is true
    i.minKey = minKey;
    i.maxKey = maxKey;
    return i;
  }

  private void loadHeader(final FileChannel ch) throws IOException {
    final ByteBuffer loadBuffer = ByteBuffer.allocate(Header.HEADER_LENGTH_BYTES);
    ch.read(loadBuffer); //maybe empty
    loadBuffer.flip();
    final Header h = Header.deserialize(loadBuffer);
    minKeyOffset = h.getMinKeyOffset();
    maxKeyOffset = h.getMaxKeyOffset();
    totalKeys = h.getTotalKeys();
    log.info("Index file at {} has {} keys", path, totalKeys);
  }

  private void loadRecords(final FileChannel ch) throws IOException {
    final ByteBuffer loadBuffer = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
    Record e;
    loadBuffer.mark();
    while (ch.read(loadBuffer) > 0) {
      loadBuffer.flip();// trim buffer to filled value
      while (loadBuffer.hasRemaining()) { // while we have not read whole buffer
        loadBuffer.mark();
        e = Record.tryDeserialize(loadBuffer);
        if (e == null) {
          loadBuffer.reset();
          break;
        }
        map.put(new ByteArrayWrapper(e.key), e.offset);
        if (e.offset == minKeyOffset) {
          minKey = e.key;
        }
        if (e.offset == maxKeyOffset) {
          maxKey = e.key;
        }
      }
      loadBuffer.mark();
    }
    log.info("Loaded {} records to memory for index {}", map.size(), path);
  }

  public void load() {
    try (final FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
      loadHeader(ch);
      loadRecords(ch);
    } catch (IOException e) {
      throw new InitialisationException("Error in loading index file: " + path + e.getMessage(), e);
    }
  }


  @Override
  public long getId() {
    return id;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public void put(byte[] key, long offset) {
    if (shouldLock) {

    }
  }

  @Override
  public long get(byte[] k) {
    if (shouldLock) {
      try {
        lock.lock();
        return doGet(k);
      } finally {
        lock.unlock();
      }
    }
    return doGet(k);
  }

  private long doGet(byte[] k) {
    Long l = map.get(k);
    return l == null ? -1 : l;
  }

  @Override
  public void flush(final boolean syncMode) throws IOException {
    //create header, dump, in loop create entry and dump

    //TODO: think about direct buffers
    ByteBuffer b = ByteBuffer.allocate(BUFFER_SIZE_BYTES);

    Set<OpenOption> opts = new HashSet<>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.WRITE));
    {
      if (syncMode) {
        opts.add(StandardOpenOption.DSYNC);
      }
    }
    //We are not allocating disk space here, so it grows after 128KB. We can optimise it.
    // RAF.setLength creates sparse file and hence doesn't guarantee disk space.
    try (FileChannel ch = FileChannel.open(path, opts)) {
      b = new Header(map.size(), minKeyOffset, maxKeyOffset).serialize();
      log.info("writing header to index file: {} bytes", b.limit());
      atomicWrite(ch, b);
      b.rewind();
            /*
            To guarantee atomic writes(which OS doesn't provide as no FS is transactional),
            we can do 2 things:
            - check free disk space and inodes in partition before every write, again this is not fullproof with small
              race window.
            - actually write 0s to the file(we can use this approach to preallocate too), this is the best guarantee
              as we are actually allocating space.
             */
      for (Map.Entry<ByteArrayWrapper, Long> e : map.entrySet()) {
        // index file has PUT and DELETE values.
        b.put(new Record(e.getKey().unwrap(), e.getValue()).serialize());
        if (b.remaining() < MAX_ENTRY_SIZE_BYTES) {
          b.flip();
          log.debug("writing chunk to index file: {} bytes", b.limit());
          atomicWrite(ch, b);
          b.rewind();
        }
      }
      if (b.position() > 0) {
        b.flip();
        log.debug("writing last chunk to index file: {} bytes", b.limit());
        atomicWrite(ch, b);
      }
    }
  }

  @Override
  public void rename(Path n) throws IOException {
    path = Files.move(path, n);
  }

  @Override
  public void lockAndSignal() {
    shouldLock = true;
    lock.lock();
  }

  @Override
  public void unlockAndSignal() {
    lock.unlock();
    shouldLock = false;
  }

  @Override
  public double getDensity() {
    long range = AppUtils.findRange(minKey, maxKey, 3);
    return (double) range / totalKeys;
  }

  public static class IndexLoader implements Bootable<Index> {
    private static final Logger log = LoggerFactory.getLogger(IndexLoader.class);
    private final Path p;

    public IndexLoader(final Path p) {
      this.p = p;
    }

    @Override
    public Index boot() {
      log.info("Booting index file at: {}", p);
      long id = FileUtils.getId(p);
      final IndexImpl i = emptyIndex(p, id);
      i.load();
      return i;
    }
  }
}
