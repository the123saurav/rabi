We keep bloom filter for L3 files.
The filters can go out of date because of deletes reaching to the file.
So we periodically need to flush bloom filter.
So we recreate it when we are invalidated during compaction.
When its recreated, we do a atomic flip to it.