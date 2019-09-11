This doc details about the compaction process.

Compaction kicks in when:
* Number of files crosses max_flushed_files(3) files. This is not configurable.
* Disk has enough space for running compaction:
  We decide it by:
  - Free space in partition > 1.1 * size of candidate_file.
  - More than 20% Free inodes. 
  else we crash.
During compaction:
```    
    candidate = highest_density_file(by just reading num_keys and range only)
    if no_files_at_L3:
        bring candidate down
    else:
        - load candidate data file in memory in form of Map.
        - find the target files where we would append.
        - create N hashmaps with ranges same as N target files.
        - iterate over all keys in the candidate's index file(note that number of entries in data file <= index file as index file stores deleted keys too)
            - lookup for corresponding entry in dataMap, if present put its key, value one of the bucket, else put key, null.
        for all target files, create batched IO writes.
        for each bucket/target file:
            for each k,v in bucket/target file:
            - Read the target files index file in memory HashMap.
              Note that this map also keeps track of num deleted entries in map. 
              Dont Grow the target file first(not holes but actually grow) as that OOS or OOIN issues surfaces here. But if we grow and fail before writing, we would be in bad state after restart.
            - start appending data to Data section of file in a loop with checks(We can go out of disk here, leaving us in inconsistent state, so perhaps check disk space again here and inodes before each batched write)
              Note that the data can be a tombstone in which case we just write to index and not data section.(if v == null)
            - update the memory HashMap for target file(its OK if we failed here) with the offset OR tombstone. IF its tombstone update num_deleted_entry
            - Touch a new temporary index file writing the range first and then Write the hashMap to index file with timestamp and then mv the new one to old one.(Its OK if we fail as we would have OLD index file, mv is atomic operation)
            - if num_deleted is huge, invalidate bloom filter.
        if some keys are left which dont fall to any target file, then:     
            if num_keys > min_orphaned_keys_during_compaction:
                create new file in L3
            else:
                merge into existing same as above(batched writes) but also updating in-memory index files' range.
                To find the target files, we use the nearest neighbour approach to find closest file.
        unlink the candidate file(its OK if we faile here)
        
Note: since we are just appending data, its OK to fail midway 
      as all it means is putting redundant data. This would be taken 
      care of during rebalancing.    
```    
        