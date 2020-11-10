package com.rabi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        Thread.currentThread().sleep(500);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
    t.interrupt();
  }

  public static List<Path> getDiskArtifact(final Path dir, final long id, final String substr) throws IOException {
    final String idStr = Long.toString(id);
    return Files.walk(dir).filter(fp ->
        fp.toString().contains(idStr) && fp.toString().contains(substr)
    ).collect(Collectors.toList());
  }
}
