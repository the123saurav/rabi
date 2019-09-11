An L3 file can be smaller in size than L2(because of orphaned keys during compaction).
Note that load balancing only run for L3 files.
There are 2 types of load balancing:
1. Partitioning: This is triggered by max_L3_file_size
   When this happens we do following: 
   - read the index file for the corresponding data file in memory
   - get the median key from above
   - get all valid data offsets for this file(which are values in index HashMap) in a HashSet
   - create a temp1/2 data and index file
   - in a loop:
        read data from temp1/2 data file
        for each key:
            - construct the offset 
            - if it is still valid:
                - add it to write batch for temp1/2
                - write the key and new offset in temp1/2 index.
        append the batch to temp1/2
   - flush the temp1/2 index file.  
   - mv the new temp files as new data file and unlink old one.   
   ANY failue is OK as the files are temp ones and unlinked on start.
   
2. Merging:   
   We should not keep lots of small files as that increases IO during compaction.
   But a file can only be small due to orpahned_keys during compaction and even for that we have a check on min values.
   Does it make sense to delete tombstones here?
   Tombstones are in index file which will anyway needs to be read.
   So its like not having an entry is same as having a tombstone.
   It definitely is taking disk space but if it grows then we will
   reclaim the disk space during partitioning.
   So right now, we are not implementing rebalancing.