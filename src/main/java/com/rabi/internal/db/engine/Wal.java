package com.rabi.internal.db.engine;

import com.rabi.internal.db.engine.wal.Entry;
import com.rabi.internal.exceptions.InvalidCheckpointException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Wal {

    /**
     * Loads a WAL represented by file path.
     */
    List<Entry> load() throws IOException;

    /**
     * appends a put entry in WAL returning offset
     *
     * @param k - key
     * @param v - value
     * @return vTime for the entry
     */
    long appendPut(byte[] k, byte[] v) throws IOException;

    /**
     * appends a delete entry in WAL returning offset
     *
     * @param k - key
     * @return vTime for the entry
     */
    long appendDelete(byte[] k) throws IOException;

    /**
     * updates the checkpoint offset
     *
     * @param offset - new checkpoint offset
     * @throws {@link InvalidCheckpointException} if offset beyond WAL's offset.
     */
    void checkpoint(long offset) throws InvalidCheckpointException;

    /**
     * get checkpoint's offset
     *
     * @return checkpointed offset
     */
    long getCheckpointOffset();

    /**
     * Get last offset of WAL.
     *
     * @return last Offset
     */
    long getLastOffset();

    /**
     * Closes WAL
     * This causes sync to disk device.
     */
    void close() throws IOException;

    void renameToTmp() throws IOException;

    void unlink() throws IOException;
}
