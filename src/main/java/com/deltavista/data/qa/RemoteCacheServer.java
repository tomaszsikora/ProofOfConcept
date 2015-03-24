package com.deltavista.data.qa;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.*;

import static com.deltavista.data.qa.CacheCommands.*;

/**
 * Created by tomek on 06.03.15.
 */
public class RemoteCacheServer {
    final DirectCache cache;
    FileChannel fc;

    public MappedByteBuffer buffer;
    RemoteCacheServer(String path, int size) throws IOException {
        cache = new DirectCache(size,5000000000L);
        fc = new RandomAccessFile(new File(path), "rw").getChannel();
        buffer = fc.map(FileChannel.MapMode.READ_WRITE,0,1024*1024);
        fc.size();
        fc.force(true);

        buffer.load();
        buffer.clear();
        buffer.force();

    }
    public void read()
    {
        buffer.position(0);
        byte operation = buffer.get();
        if(operation==0)
        {
            return;
        }
        switch (operation)
            {
                case GET:
                    buffer.position(1);
                    int mainId = buffer.getInt();
                    byte[] record = cache.get(mainId);
                    buffer.position(1);
                    buffer.putInt(record.length);
                    buffer.put(record);
                    buffer.position(0);
                    buffer.put(READY);
                    break;
                case PUTORUPDATE:
                    buffer.position(1);
                    int key = buffer.getInt();
                    int size = buffer.getInt();
                    byte[] newRecord = new byte[size];
                    buffer.get(newRecord);
                    cache.putOrUpdate(key,newRecord);
                    buffer.position(0);
                    buffer.put(READY);
                    break;
                case DELETE:
                    buffer.position(1);
                    int id = buffer.getInt();
                    cache.delete(id);
                    buffer.position(0);
                    buffer.put(READY);
                    break;
                case REMAINING:
                    buffer.position(1);
                    buffer.putLong(cache.getRemaining());
                    buffer.position(0);
                    buffer.put(READY);
                    break;
                default:
                    break;

            }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        RemoteCacheServer server = new RemoteCacheServer(args[0], Integer.valueOf(args[1]));
        while(true)
        {
           server.read();
           Thread.yield();

        }
    }

}
