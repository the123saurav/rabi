package com.rabi.internal.db.engine;

import java.io.IOException;

public interface Bootable<T> {
  T boot() throws IOException;
}
