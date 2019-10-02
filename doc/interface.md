This lists the public API of rabi

The constructor allows passing a Config.

#Open
If DB is already opened return it, else take a lock.
We do DCL and use singleton instance of DB so that multiple
concurrent calls return same DB.
`Future/CompletableFuture Open()`
check operations.md
There is no timeout here.
This step marks DB state as bootstrap -> running.

#Get

`Option(byte[]) get(byte[] key)`
Checks is db is in running state.
Valid DB states are:
- bootstrap
- running
- terminating
- terminated

Gets the value associated with the key.

#Put

`Put(byte[] key, byte[] value)`
Checks is db is in running state.
This is an idempotent operation.
Write to WAL(source of truth).
Delete if entry in deleted section of map.
Put to data section of map.

#Delete
Checks is db is in running state.
`Delete(byte[] key)`

This is an idempotent operation.
Write to WAL(source of truth).
Create an entry in deleted secion of memtable.
Delete if present from memtable.

#Stop

`Stop()`

Marks state as terminating in caller thread to deny all
reads and writes and then spawns a thread to carry out shutdown activities
thereby not blocking the caller.
Check shutdown.md

#Stats
Checks is db is in running state.

`Stat Stats() & Stats(String statKey)`

Returns a stat to user.


