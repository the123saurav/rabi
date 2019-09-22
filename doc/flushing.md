We flush immutable L<sub>1</sub> memtables to
disk when number of files reaches a threshold OR there
is danger of running OOM.

At flush, we:
* write memtable to file.
* checkpoint and update the wal pointer.
* check if WAL needs to be rotated.


### How do we write the memtable to file? 
- touch a temp data and index file
- seek to offset 8
- for k, v in memtable: 
    - construct the record format and get the next offset. 
    - add to batch for IO
    - add an entry to indexMap with offset constructed OR 0(tombstone)
    - if batch size reached:
            - flush to disk
- flush index to file along with range            
- rename/mv the data and index file
- give memtable to GC.