package com.rabi.internal.db.engine.task.boot;

import com.rabi.exceptions.InitialisationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes file.
 */
public class FileCleanupTask extends BaseTask {

  private final Path file;
  private static final Logger LOG = LoggerFactory.getLogger(FileCleanupTask.class);

  public FileCleanupTask(final Path file) {
    this.file = file;
  }

  @Override
  public void run() {
    LOG.debug("[FileCleanupTask] deleting file: " + file);
    //doesn't work for directories.
    try {
      Files.delete(file);
    } catch (IOException e) {
      throw new InitialisationException("Unable to clean file: " + file + "\n" + e.getMessage(), e);
    }
  }
}
