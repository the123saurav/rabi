package com.rabi;

import com.rabi.internal.db.DBImpl;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

/**
 * DBFactory creates DB instances.
 * There is one DB instance per data directory.
 * There could be multiple DB instances existing at the same time.
 */
public final class DBFactory {

  private DBFactory() {
  }

  /**
   * There can be multiple db instance in an app each identified by a data-dir.
   *
   * @param dataDir - directory for all rabi data
   * @param logger  - logger
   * @return DB instance which needs to be opened.
   * <p>
   */
  public static DB getInstance(@Nonnull final String dataDir, @Nonnull final Logger logger) {
    return DBImpl.get(dataDir, logger);
  }
}
