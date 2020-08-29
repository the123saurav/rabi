package com.rabi.internal.db;

import com.rabi.internal.stats.Counter;
import com.rabi.internal.stats.Quantum;
import com.rabi.internal.stats.Snapshot;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * A singleton class with all Stats. Note that some stats may be costly
 * and can be turned off in Config.
 * DB has an instance of this.
 * - num_immutable_memtables
 * - mutable_memtable_size
 * - immutable_memtable_size
 * - WAL checkpoint backlog
 * - num_L2_files
 * - num_L3_files
 * - last_compaction_start
 * - last_compaction_duration
 * - num_compactions
 * - num_compactions_in_last_hour
 * - last_rebalance_start
 * - last_rebalance_duration
 * - num_rebalances
 * - num_rebalances_in_last_hour
 * - last_flush_start
 * - last_flush_duration
 * - num_flushes
 * - num_flushes_in_last_hour
 * - open_time_duration
 * <p>
 * The stats are set by DB instance upon events.
 * This can happen via a system wide event queue OR a pub-sub mechanism.
 */
//TODO think if we need this public
public final class Stats {

  //maybe use counters, snapshot classes
  private Snapshot<Integer> numImmutableMemtables;
  private Snapshot<Integer> mutableMemtableSizeBytes;
  private Snapshot<Integer>[] immutableMemtablesSizeBytes;
  private Snapshot<Integer> walCheckPointBacklog;
  private Snapshot<Integer> numL2Files;
  private Snapshot<Integer> numL3Files;
  private Snapshot<LocalDateTime> lastCompactionStart;
  private Snapshot<Duration> lastCompactionDurationMs;
  private Counter numCompactions;
  private Quantum numCompactionsInLastHour;
  private Snapshot<LocalDateTime> lastRebalanceStart;
  private Snapshot<Integer> lastRebalanceDurationMs;
  private Counter numRebalances;
  private Quantum numRebalancesInLastHour;
  private Snapshot<LocalDateTime> lastFlushStart;
  private Snapshot<Integer> lastFlushDurationMs;
  private Counter numFlushes;
  private Quantum numFlushesInLastHour;

  private Logger log;

  //No need to ensure singleton as this is not public class.
  public Stats(Logger logger) {
    log = logger;
  }

  Map<String, String> pull() {
    Map<String, String> m = new HashMap<>();
    //convert to map.
    return m;
  }

}
