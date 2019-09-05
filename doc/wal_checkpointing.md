The WAL is checkpointed everytime we flush a memtable
to disk. The WAL is read at the beginning to 
construct mutable memtable.
Note that if we crashed because of OOM then resconstructing memtable from WAL at restart
will keep crashing, hence we can stall writes at 2 condition:
- max_memtables
- memory available is less

Note that before flushing you might have had max_memtables + 1
memtables but at bootstrap we construct 1 memtable and flush it.
It doesn't matter as total memory is same.

#What happens at checkpoint?

At regular operation/bootstrap
   push the oldest memtable to disk
   and persist checkpoint pointer of WAL.
   How to make this atomic??? 
   If we cant make it atomic, then at worse we would
   have duplicate data which is okay as that is like a
   redundant `Put`.
   
   
# How to trim WAL log?

   
