We mainitain stats to be queried by user to get idea of
- app state
- load
- performance

Some stats to return would be:
- num_immutable_memtables
- mutable_memtable_size
- immutable_memtable_size
- WAL checkpoint backlog
- num_L2_files
- num_L3_files
- last_compaction_start
- last_compaction_duration
- num_compactions
- num_compactions_in_last_hour
- last_rebalance_start
- last_rebalance_duration
- num_rebalances
- num_rebalances_in_last_hour
- last_flush_start
- last_flush_duration
- num_flushes
- num_flushes_in_last_hour
- open_time_duration



We don't give metrics on throughput rate for APIs as of now.
