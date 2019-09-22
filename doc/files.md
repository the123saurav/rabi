We have following files in our system.
We are not using compression OR checksums as of now.

* L<sub>2</sub> files.
* L<sub>3</sub> files.
* L<sub>2</sub> index file.
* L<sub>3</sub> index file.
* MANIFEST file. 
* wal(can be more than 1) - wal_pointer persisted at top of 8bytes fixed size.

All temp files have .temp as extension.
All L2 files have .l2.[data|index]
All L3 files have .l3.[data|index]

### Data file layout:
<key_len><val_len><key><val>
8 + 8 + m + n length

### Index file layout:
- num_deleted???

8 bytes free at start.
<num_keys> (8)
<min_key_offset> (8)
<max_key_offset> (8)
<key_len><key><offset>

Note that we dont write anything at first 8 bytes.
This is because the tombstone offset in index is set to point to 0 offset.

### WAL Layout
Records Put and Delete
first 8 bytes is for offset of entry next to checkpointed.
<ms_from_epoch><op_type><key_len><key>[<val_len><val>](Only for delete)
8+1+8+m+8+n

### MANIFEST
Should we have key range here??? Brings in complexity in guarantees, so lets keep it this way.
Keeping in MANIFEST would reduce IO.
Maybe something like for only L3 files:
<min_key_len><min_key><max_key_len><max_key><file_name_len><file_name>

