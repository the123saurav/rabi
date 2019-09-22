This doc describes the Delete API.

For delete, we delete from mainMap and put this entry in deletedMap.
We keep marker instead of just deleting from Hashmap, so that for future calls
we don't end up returning a value from lower levels.
The deletes are processed like stale updates during compaction.

Note that absence of key at L3 only means deleted but not on other levels.
Even at L3 we can have a tombstone flag in index file. 
The tombstones are reaped during rebalancing.

