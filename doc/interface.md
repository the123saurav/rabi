This lists the public API of rabi

The constructor allows passing a Config.

#Start

`Future/CompletableFuture Start()`
- cleans all temp files.
- constructs memtable from WAL.
- bootstraps the DB loading indices of L<sub>2</sub> 
- bloom filter of L<sub>3</sub>.
There is no timeout here.

#Get

`Option(byte[]) get(byte[] key)`

Gets the value associated with the key.

#Put

`Put(byte[] key, byte[] value)`

This is an idempotent operation.
Write to WAL(source of truth).
Delete if entry in deleted section of map.
Put to data section of map.

#Delete

`Delete(byte[] key)`

This is an idempotent operation.
Write to WAL(source of truth).
Create an entry in deleted secion of memtable.
Delete if present from memtable.

#Stop

`Stop()`

Marks state as shutdown in caller thread to deny all
reads and writes and then spawns a thread to carry out shutdown activities
thereby not blocking the caller.
Check shutdown.md


