This doc describes the Delete API.

For delete, we just follow the same as update.
We update the memtable entry saying the record 
is deleted. We keep marker instead of just deleting from Hashmap, so that for future calls
we don't end up returning a value from lower levels.
The deletes are processed like stale updates during compaction moving now-invalid values to deletionlist file.
The deletelist file is read during rebalancing.
