package com.rabi.internal.db.engine.util;

import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import com.rabi.internal.types.ByteArrayWrapper;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileUtils {

  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  public static int safeAppend(final FileChannel ch, final ByteBuffer b) throws IOException {
    final ByteBuffer dummy = ByteBuffer.allocate(b.limit()); // default val of byte array is 0
    log.debug("dummy buffer: {}/{}", dummy.position(), dummy.limit());
    log.debug("channel pos before: {}", ch.position());
    int written = ch.write(dummy); // fails if space/inode/permissions inadequate
    log.debug("dummy buffer write: {}", written);
    if (written != b.limit()) {
      // TODO should it truncate?
      throw new RuntimeException(String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written));
    }
    //rewind and actual write.
    ch.position(ch.position() - written);
    log.debug("channel pos after: {}", ch.position());
    written = ch.write(b);
    assert (written == b.limit()) : String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written);
    return written;
  }

  public static long getId(final Path p) {
    String[] s = p.toString().split("/");
    s = s[s.length - 1].split("\\.");
    return Long.parseLong(s[0]);
  }

  public static Pair<Data, Index> flushLevelFiles(final List<Pair<byte[], byte[]>> recordSet,
                                                  final Path dataDir, final long id,
                                                  final boolean syncMode, final String level) throws IOException {
    final Data d = flushDataFile(recordSet, dataDir, id, syncMode, level);
    final Index i = flushIndexFile(recordSet, dataDir, id, syncMode, level);
    d.rename(Paths.get(dataDir.toString() + "/" + id + "." + level + ".data"));
    i.rename(Paths.get(dataDir.toString() + "/" + id + "." + level + ".index"));
    log.info("Renamed data and index files after flush");
    return ImmutablePair.of(d, i);
  }

  private static Data flushDataFile(final List<Pair<byte[], byte[]>> recordSet,
                                    final Path dataDir, final long id, final boolean syncMode,
                                    final String level) throws IOException {
    final Path p = Paths.get(dataDir.toString() + "/" + id + "." + level + ".data.tmp");
    log.debug("Data file path during flush would be: {}", p);
    final Data d = new DataImpl(p, FileUtils.getId(p), 8); // TODO: should we use 0 here?
    d.flush(recordSet, syncMode);
    log.info("Flushed datafile {} to {}", id, p);
    return d;
  }

  private static Index flushIndexFile(final List<Pair<byte[], byte[]>> entries, Path dataDir, long id,
                                      boolean syncMode, final String level) throws IOException {
    byte[] tmp = new byte[256];
    Arrays.fill(tmp, (byte) 255);
    ByteArrayWrapper minKey = new ByteArrayWrapper(tmp);
    ByteArrayWrapper maxKey = new ByteArrayWrapper(new byte[] {(byte) 0});
    long minKeyOffset = 0;
    long maxKeyOffset = 0;
    Map<ByteArrayWrapper, Long> m = new HashMap<>();
    ByteArrayWrapper k;
    long fileOffset = 8; // first 8 bytes are free
    long currOffset;

    for (final Pair<byte[], byte[]> e : entries) {
      k = new ByteArrayWrapper(e.getLeft());
      currOffset = 0;
      if (e.getRight() != null) {
        currOffset = fileOffset;
        fileOffset += 1 + 2 + e.getLeft().length + e.getRight().length;
        //minkey/maxkey is one of the keys in data file
        if (k.compareTo(minKey) <= 0) {
          minKey = k;
          minKeyOffset = currOffset;
        }
        if (k.compareTo(maxKey) >= 0) {
          maxKey = k;
          maxKeyOffset = currOffset;
        }
      }
      m.put(k, currOffset);
    }
    final Path indexPath = Paths.get(dataDir.toString() + "/" + id + "." + level + ".index.tmp");
    log.debug("Index file path upon flush would be: {}", indexPath);
    final long indexId = FileUtils.getId(indexPath);
    final Index i = IndexImpl.loadedIndex(indexPath, indexId, m, minKey.unwrap(),
        minKeyOffset, maxKey.unwrap(), maxKeyOffset);
    i.flush(syncMode);
    return i;
  }
}
