This lists the public API of rabi

The constructor allows passing a Config.

#Start

`Future/CompletableFuture Start()`
bootstraps the DB loading indices of L<sub>2</sub> 
and bloom filter of L<sub>3</sub>.
There is no timeout here.

#Get

`Option(byte[]) get(byte[] key)`

Gets the value associated with the key.

#Put

`Put(byte[] key, byte[] value)`

This is an idempotent operation.

#Delete

`Delete(byte[] key)`

This is an idempotent operation.

#Stop

`Stop()`

Marks state as shutdown in caller thread to deny all
reads and writes and then spawns a thread to carry out shutdown activities
thereby not blocking the caller.
Check shutdown.md


