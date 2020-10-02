package com.rabi;

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
}
