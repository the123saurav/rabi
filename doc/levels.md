There are `4` levels in our tree: 

###<u>L</u><sub>0</sub>

* This is the mutable memory resident Hashmap from CustomByteArray to byte[].

It has below definition:
Each memtable is backed by WAL which is known to it at creation time.
The WAL offset is set by Put if it finds WAL is full.
```java
class com.rabi.memtable.MemTable{
    private Map<com.rabi.types.ByteArrayWrapper, byte[]> m;
    private byte[] minKey; //this will be used in  L1 for filtering out reads
    private byte[] maxKey;
    private long walEndOffset; //for checkpointing
    private File wal; //there could be 2 WALs, so we need to know which to checkpoint.
    private Set<com.rabi.types.ByteArrayWrapper> deleted;
}
```

###<u>L</u><sub>1</sub>

* This is the immutable memory resident L<sub>0</sub>.
* These all are files that would be dumped to disk in the background.
* All the files here are roughly of same size as we reply on size to dump them to disk.
* We limit the number of L<sub>1</sub> files. On hitting that limit, we `halt writes` and push these/wait for push to complete on disk.


###<u>L</u><sub>2</sub>

* Persistent version of L<sub>1</sub> files and hence they are mostly of same size.
* There is a limit on number of files - `max_flushed_files`
* Have overlap of keys.
* The index for these files lie in memory as they are small.
* These are eliminated during compaction. We compact only 1 index file at any time.
  When there are no files in L<sub>3</sub>, then we dont just move file
  down blindly. We can merge few and then move.
  Less number of files in L<sub>3</sub> is good as it leads to less IO.
  
  
###<u>L</u><sub>3</sub>  
  
* This has files of varying size.
* Their ranges don't overlap. So while reading we open only 1 file.
* Only bloom filters are in memory, these are created during boot and updated during compaction.  
  The bloom filter may need to be reconstructed during partitioning.
* The files here partiticpate in load_balancing.
* We rely on OS cache for data caching(e.g index, data)
  
  
  

