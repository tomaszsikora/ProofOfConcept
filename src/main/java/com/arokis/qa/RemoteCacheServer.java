package com.arokis.qa;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by tomek on 06.03.15.
 */
public class RemoteCacheServer {
    DirectCache cache;
    FileChannel fc;

    public MappedByteBuffer buffer;
    RemoteCacheServer(String path) throws IOException {
        cache = new DirectCache(1);
        fc = new RandomAccessFile(new File(path), "rw").getChannel();
        buffer = fc.map(FileChannel.MapMode.READ_WRITE,0,128*1024);
        fc.size();
        fc.force(true);

        buffer.load();
        buffer.clear();
        buffer.force();

    }
    public void read() throws IOException {
        buffer.position(0);
        byte operation = buffer.get();
        if(operation!=0)
        {
            switch (operation)
            {
                case (byte)2:
                    buffer.position(1);
                    int key = buffer.getInt();
                    int size = buffer.getInt();
                    byte[] record = new byte[size];
                    buffer.get(record);
                    cache.putOrUpdate(key,record);
                    buffer.position(0);
                    buffer.put((byte)0);
                    break;
                case (byte)4:
                    buffer.position(1);
                    buffer.putLong(cache.getRemaining());
                    buffer.position(0);
                    buffer.put((byte)0);
                    break;
                default:
                    break;

            }

        }


    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RemoteCacheServer server = new RemoteCacheServer(args[0]);
        while(true)
        {
         //  server.buffer.load();
           server.read();
           Thread.yield();

        }
    }

}
