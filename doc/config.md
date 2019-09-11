Below are the list of config params:

* memtable_max_size - max size of mutable memtable(this should be grwater than 50MB so that we dont keep compacting files)
* max_memtables
* max_flushed_files - this cant be set by user.
* min_orphaned_keys_during_compaction - cant be set.