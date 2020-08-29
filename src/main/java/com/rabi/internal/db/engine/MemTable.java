package com.rabi.internal.db.engine;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;

public interface MemTable extends Bootable<Void> {

  long size();

  void put(byte[] k, byte[] v) throws IOException;

  byte[] get(byte[] k);

  void delete(byte[] k) throws IOException;

  void allowMutation();

  void disallowMutation();

  void close() throws IOException;

  List<Pair<byte[], byte[]>> export();

  long getId();

  void cleanup() throws IOException;
}