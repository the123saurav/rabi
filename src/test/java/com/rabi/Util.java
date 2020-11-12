package com.rabi;

import com.rabi.internal.db.engine.Data;
import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.data.DataImpl;
import com.rabi.internal.db.engine.index.IndexImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Util {

  private Util() {
  }

  public static String getAlphaNumericString(int n) {

    String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    StringBuilder sb = new StringBuilder(n);

    for (int i = 0; i < n; i++) {
      int index = (int) (AlphaNumericString.length() * Math.random());
      sb.append(AlphaNumericString.charAt(index));
    }

    return sb.toString();
  }

  public static void doInterruptThread(final Thread t) {
    if (!t.isAlive()) {
      try {
        Thread.currentThread().sleep(2000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
    t.interrupt();
  }

  public static List<Path> getDiskArtifact(final Path dir, final String... subStrings) throws IOException {
    return Files.walk(dir).filter(fp -> {
          final String pathStr = fp.toString();
          return Arrays.stream(subStrings).allMatch(pathStr::contains);
        }
    ).collect(Collectors.toList());
  }

  public static Index loadIndexFromDisk(final Path p) {
    return new IndexImpl.IndexLoader(p).boot();
  }

  public static Data loadDataFromDisk(final Path p) throws IOException {
    final DataImpl data = (DataImpl) new DataImpl.DataBooter(p).boot();
    data.loadValues();
    return data;
  }
}
