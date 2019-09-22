We keep bloom filter for L3 files.
These are created at boot time.
The filters can go out of date because of deletes reaching to the file.
So we periodically need to flush bloom filter.
We recreate it when we are invalidated during compaction/rebalancing.
When its recreated, we do a atomic flip to it.