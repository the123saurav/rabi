User calls the Open(config) passing the directory as only required argument.

Open(): //returns singleton DB via DCL for concurrent/future calls.
    - validate config
    - cleanup()
    - bootstrap() //check interface.md
    - loadRuntime()
    - mark DB state as running
    - installs shutdown hook
    - returns a DB instance(no interface here)
    
cleanup():
    - removes all tmp files.    
    
bootstrap():
    - marks state as bootstrapping
    - constructs memtable from WAL.
    - bootstraps the DB loading indices of L<sub>2</sub> 
    - bloom filter of L<sub>3</sub>.
    
loadRuntime():
    - starts flushing routine(this also takes care of WAL checkpointing and rotation)
    - starts compaction routine.(this touches the same file as below, so is mutually exclusive)
    - starts partitioning routine. 
    Note that compaction touches both L2 and L3 files,   
    partitioning touches only L3,
    flushing doesn't touch any existing file but creates new L2 files.
    Flushing results in compaction and its OK for both to run parallely.
    If compaction is slower then the rate at which flushing is producing file, we can stall writes.
    All 3 can be aborted midway as they either make use of temp files(flushing)
    OR can lead to duplicate data.
    