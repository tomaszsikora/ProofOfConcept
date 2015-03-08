package com.arokis.qa;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import static com.arokis.qa.CacheCommands.*;

/**
 * Created by tomek on 05.03.15.
 */
public class RemoteCache implements IDirectCache {

    MappedByteBuffer buffer;

    RemoteCache(String path) throws IOException {

        FileChannel fc = new RandomAccessFile(new File(path), "rw").getChannel();
        buffer = fc.map(FileChannel.MapMode.READ_WRITE,0,128*1024);
        fc.force(true);
        buffer.load();
        buffer.force();
    }

    @Override
    public long getRemaining() {
        buffer.position(0);
        buffer.put(REMAINING);
        while(true)
        {
            buffer.position(0);
            if(buffer.get()==READY)
            {
                buffer.position(1);
                long remaining = buffer.getLong();
                return remaining;
            }
            Thread.yield();
        }
    }

    @Override
    public byte[] get(int id) {
        buffer.position(1);
        buffer.putInt(id);
        buffer.position(0);
        buffer.put(GET);
        while(true)
        {
            buffer.position(0);
            if(buffer.get()==READY)
            {
                buffer.position(1);
                int size = buffer.getInt();
                byte[] response = new byte[size];
                buffer.get(response);
                return response;
            }
            Thread.yield();
        }
    }

    @Override
    public void putOrUpdate(int key, byte[] record) {
        buffer.position(1);
        buffer.putInt(key);
        buffer.putInt(record.length);
        buffer.put(record);
        buffer.position(0);
        buffer.put(PUTORUPDATE);
        while(true)
        {
            buffer.position(0);
            if(buffer.get()==READY)
            {
                return;
            }
            Thread.yield();
        }


    }

    @Override
    public void delete(int maindId) {
        buffer.position(1);
        buffer.putInt(maindId);
        buffer.position(0);
        buffer.put(DELETE);
        while(true)
        {
            buffer.position(0);
            if(buffer.get()==READY)
            {
                return;
            }
            Thread.yield();
        }

    }
}
