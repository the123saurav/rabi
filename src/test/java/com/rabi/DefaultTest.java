package com.rabi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DefaultTest class contains test for basic get and put operation
 * for the db. This helps to load the library and run/debug issues.
 * TODO(samdgupi): Better naming and also add some e2e like tests
 */
public class DefaultTest {
  static Logger testLogger = LoggerFactory.getLogger(TestConfig.LOGGER_NAME);

  /**
   * testPutAndGet will test basic put and get operation.
   *
   * @param tempDir temporary directory injected for creating db
   */
  @Test
  public void testPutAndGet(@TempDir Path tempDir) {
    byte[] validKey = "valid".getBytes();
    byte[] value = "value".getBytes();
    byte[] invalidKey = "invalid".getBytes();

    DB testDB = DBFactory.getInstance(tempDir.toString(), testLogger);
    Config.ConfigBuilder configBuilder = new Config.ConfigBuilder();


    assertDoesNotThrow(() -> {
      CompletableFuture<Void> isOpen = testDB.open(configBuilder.build());
      isOpen.get(TestConfig.OPEN_TIMEOUT_SEC, TimeUnit.SECONDS);
    });
    assertDoesNotThrow(() -> testDB.put(validKey, value));
    // TODO(samdgupi) This is a stub right now as get is not implemented
    assertArrayEquals(new byte[0], testDB.get(validKey));
    assertArrayEquals(new byte[0], testDB.get(invalidKey));
  }
}