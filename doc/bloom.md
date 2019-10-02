We keep bloom filter for L3 files.
These are created at boot time.
They load index file and inserts all key to the filter(not tombstone ofcourse).
The filters can go out of date because of deletes reaching to the file.
So we periodically need to flush bloom filter.
We recreate it when we are invalidated during compaction/rebalancing.
When its recreated, we do a atomic flip to it.