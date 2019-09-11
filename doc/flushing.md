We flush immutable L<sub>1</sub> memtables to
disk when number of files reaches a threshold OR there
is danger of running OOM.

At flush, we:
* write memtable to file.
* checkpoint and update the wal pointer.
* check if WAL needs to be rotated.