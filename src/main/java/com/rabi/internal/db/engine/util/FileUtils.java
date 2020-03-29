package com.rabi.internal.db.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);
    private FileUtils(){}

    public static void atomicWrite(FileChannel ch, ByteBuffer b) throws IOException {
        ByteBuffer dummy = ByteBuffer.allocate(b.limit()); // default val of byte array is 0
        log.info("dummy buffer: {}/{}", dummy.position(), dummy.limit());
        log.info("channel pos: {}", ch.position());
        int written = ch.write(dummy); // fails if space/inode/permissions inadequate
        log.info("dummy buffer write: {}", written);
        if(written != b.limit()){
            throw new RuntimeException(String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written));
        }
        //rewind and actual write.
        ch.position(ch.position() - written);
        log.info("channel pos: {}", ch.position());
        written = ch.write(b);
        assert(written == b.limit()) : String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written);
    }
}
