package com.rabi.internal.db.engine;

import java.io.IOException;

public interface MemTable extends Loadable {

    void put(byte[] k, byte[] v) throws IOException;

    void delete(byte[] k) throws IOException;

    void allowMutation();

    void disallowMutation();

    void close() throws IOException;
}