This document describes the Put API.

We first check if writes are stalled, retruning error.
Then we write to WAL and fsync it.
Then we just update the L<sub>0</sub> memtable,
update the keyrange if needed, this operation should never fail
other than OOM/process crashing in which case we have already recorded in WAL
and will play it on restart. 
During updating memtable, we first remove from deletedSet and then add to map.

Note that after we Put:
if memtable_full():
    - update mutable table's last WAL offset
    - move mutable to immutable memtable list
    - check if WAL needs to be rotated.

updating the WAl offset