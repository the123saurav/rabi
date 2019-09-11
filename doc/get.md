This document describes the flow for Get operation.

We first search in L<sub>0</sub>, then 
L<sub>1</sub> files filtering on key range.
Then we look into L<sub>2</sub> files index filtering on key range and
finally L<sub>3</sub> files bloom filter.

If we find it at L<sub>2</sub>, then we open that file, seek to the offset
and read it.

For L<sub>3</sub> files, we first load the index in memory from file and then do same as above for L<sub>2</sub>.
We rely on OS file cache for fast IO.
