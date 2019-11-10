package com.rabi.internal.db.engine.task.boot;

import com.rabi.exceptions.InitialisationException;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Removes file.
 */
public class FileCleanupTask extends BaseTask {

    private final Path file;
    private final Logger log;

    public FileCleanupTask(Path file, Logger logger) {
        this.file = file;
        this.log = logger;
    }

    @Override
    public void run() {
        log.debug("[FileCleanupTask] deleting file: " + file);
        //doesn't work for directories.
        try {
            Files.delete(file);
        } catch (IOException e) {
            throw new InitialisationException("Unable to clean file: " + file + "\n" + e.getMessage(), e);
        }
    }
}
