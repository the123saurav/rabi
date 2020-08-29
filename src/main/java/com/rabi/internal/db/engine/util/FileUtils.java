package com.rabi.internal.db.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);
    private FileUtils(){}

    public static void atomicWrite(final FileChannel ch, final ByteBuffer b) throws IOException {
        final ByteBuffer dummy = ByteBuffer.allocate(b.limit()); // default val of byte array is 0
        log.debug("dummy buffer: {}/{}", dummy.position(), dummy.limit());
        log.debug("channel pos: {}", ch.position());
        int written = ch.write(dummy); // fails if space/inode/permissions inadequate
        log.debug("dummy buffer write: {}", written);
        if(written != b.limit()){
            throw new RuntimeException(String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written));
        }
        //rewind and actual write.
        ch.position(ch.position() - written);
        log.debug("channel pos: {}", ch.position());
        written = ch.write(b);
        assert(written == b.limit()) : String.format("failed to write all %d bytes, wrote: %d bytes only", b.limit(), written);
    }

    public static long getId(final Path p) {
        String[] s = p.toString().split("/");
        s = s[s.length -1].split("\\.");
        return Long.parseLong(s[0]);
    }
}
