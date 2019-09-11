The WAL is checkpointed everytime we flush a memtable
to disk. The WAL is read at the beginning to 
construct only mutable memtable.
Note that if we crashed because of OOM then resconstructing memtable from WAL at restart
will keep crashing, hence we can stall writes at 2 condition:
- max_memtables
- memory available is less

Note that before flushing you might have had max_memtables + 1
memtables but at bootstrap we construct all memtable(note that the checkpint is lost as thats stored in memory but we can rely on memory size) and flush it.
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

http://man7.org/linux/man-pages/man2/fallocate.2.html   
Note this works only on Linux >3.14 and ext4 and XFS.
We may need to set different flags.
Maybe we can check error(EINVAL) and fallback 
to an alternative method where we generate a new WAL log,
close the old one for writing and then once old one is checkpointed
remove it. But for this the old one should be stopped
only when we generate new memtable.
From after rotation, we write to new file (identified
by higher timestamp). In case we crash, at boot we would see 2 files and we read both and open only 1 for writing.