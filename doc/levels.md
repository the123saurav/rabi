There are `4` levels in our tree: 

###L<sub>0</sub>

* This is the mutable memory resident Hashmap from CustomByteArray to byte[].

It has below definition:
```java
class L0{
    private Map<ByteArrayWrapper, byte[]> m;
    private byte[] minKey; //this will be used in  L1 for filtering out reads
    private byte[] maxKey;
    private long walEndOffset; //for checkpointing
}
```

###L<sub>1</sub>

* This is the immutable memory resident L<sub>0</sub>.
* These all are files that would be dumped to disk in the background.
* All the files here are roughly of same size as we reply on size to dump them to disk.
* We limit the number of L<sub>1</sub> files. On hitting that limit, we `halt writes` and push these/wait for push to complete on disk.


###L<sub>2</sub>



