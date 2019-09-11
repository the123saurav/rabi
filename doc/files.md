We have following files in our system.
We are not using compression OR checksums as of now.

* L<sub>2</sub> files
* L<sub>3</sub> files
* L<sub>2</sub> index file
* L<sub>3</sub> index file
* MANIFEST file 
* delete_list - to delete keys because of update/delete.
* wal(can be more than 1) - wal_pointer persisted at top of 8bytes fixed size.

Data file layout:

- min_key
- max_key
- num_keys
- deleted_keys

Note that we dont write anything at first 8 bytes.
This is because the tombstone offset in index is set to point to 0 offset.



