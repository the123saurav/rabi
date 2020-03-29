package com.rabi.internal.db.engine;

import java.io.IOException;
import java.nio.file.Path;

public interface Index {

    void put(byte[] key, long offset);

    long get(byte[] k);

    void overwrite(Path p, boolean syncMode) throws IOException;

    void rename(Path n) throws IOException;
}
