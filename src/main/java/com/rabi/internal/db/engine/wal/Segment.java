package com.rabi.internal.db.engine.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A segment file name is of format: <ts>.0.wal
 */
public class Segment {

  private static final int LOAD_SIZE_BYTES = 128 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(Segment.class);

  private Path path;
  private final boolean sync;
  /*
      File channels are safe for use by multiple concurrent threads.
      The close method may be invoked at any time, as specified by the Channel interface.
      Only one operation that involves the channel's position or can change its
      file's size may be in progress at any given time; attempts to initiate a
      second such operation while the first is still in progress will block until
      the first operation completes. Other operations, in particular those that
      take an explicit position, may proceed concurrently; whether they in fact
      do so is dependent upon the underlying implementation and is therefore unspecified.
   */
  private FileChannel writer; //thread safe

  public Segment(Path p, boolean s) {
    path = p;
    sync = s;
  }

  List<Record> load() throws IOException {
    final List<Record> records = new ArrayList<>();

    //read records from disk resident segment file if it exists
    if (Files.exists(path)) {
      try (final FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {

        final ByteBuffer loadBuffer = ByteBuffer.allocateDirect(LOAD_SIZE_BYTES);
        Record e;
        long lastPos;
        long numRead;
        long totalRead = 0;

        while ((numRead = ch.read(loadBuffer)) > 0) {
          totalRead += numRead;
          lastPos = loadBuffer.position();
          LOG.debug(String.format("last position: %d, totalRead: %d", loadBuffer.position(), totalRead));
          loadBuffer.rewind(); //always read from start of buffer
          while (loadBuffer.position() < lastPos) {
            loadBuffer.mark();
            e = Record.tryDeserialize(loadBuffer);
            if (e == null) {
              loadBuffer.reset();
              LOG.debug(path.getFileName() + " - buffer underflow, position is: " + loadBuffer.position() + " reading more data");
              break;
            }
            records.add(e); //can we optimise this by adding in batches??
          }
          loadBuffer.compact();
        }
      }
    }

    makeWritable();
    LOG.info("loaded " + records.size() + " records from segment: " + path.getFileName());
    return records;
  }

  private void makeWritable() throws IOException {
    LOG.debug("making segment file: " + path.getFileName() + " writable");
    if (sync) {
      writer = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.DSYNC);
    } else {
      writer = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }

  /**
   * ThreadSafe, based on user config we flush now OR later
   * OR rely on OS.
   * <p>
   * It is okay for segment to be not ordered as we guarantee
   * ordering during load.
   *
   * @param k - key
   * @param v - value
   * @throws IOException
   */
  void appendPut(byte[] k, byte[] v) throws IOException {
    Record e = new Record(k, v, Instant.now().toEpochMilli());
    ByteBuffer payload = e.serialize();
    payload.rewind();
    LOG.debug("appending put to segment: " + path.getFileName() + " with size: " + payload.limit());
    int w = writer.write(payload);
    LOG.debug("wrote " + w + " bytes");
  }

  /**
   * @param k - key
   * @throws IOException
   */
  void appendDelete(byte[] k) throws IOException {
    Record e = new Record(k, Instant.now().toEpochMilli());
    writer.write(e.serialize());
  }

  void close() throws IOException {
    if (!sync) {
      writer.force(true);
    }

    writer.close(); //idempotent
  }

  void renameToTmp() throws IOException {
    path = Files.move(path, Paths.get(path.getFileName().toString() + ".tmp"));
  }

  void unlink() throws IOException {
    Files.deleteIfExists(path);
  }
}
