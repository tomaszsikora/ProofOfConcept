package com.deltavista.data.structures.cache.direct;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by t.sikora on 2015-03-09.
 */
public class DirectIntLongMap
{
    private final ByteBuffer intKey;

    private final ByteBuffer longValue;

    private final int size;

    private int firstFree;

    public DirectIntLongMap(int size)
    {
        firstFree = 0;
        this.size = size;
        intKey = ByteBuffer.allocateDirect(size * 4);
        longValue = ByteBuffer.allocateDirect(size * 8);
    }

    public int size()
    {
        return size;
    }

    public int remaining()
    {
        return size - firstFree;
    }

    public  int getOffset(int mainId)
    {
        return find(mainId);
    }

    public  long get(int key)
    {
        int index = find(key);
        if(index<0)
        {
            return 0;
        }
        return getLong(index);
    }

    public  void put(int key, long record)
    {
        final int index = find(key);
        if(index<0 || firstFree==0)
            {
                intKey.position(firstFree * 4);
                intKey.putInt(key);
                longValue.position(firstFree * 8);
                longValue.putLong(record);
                firstFree++;
            }
        else
            {
                longValue.position(index * 8);
                longValue.putLong(record);
            }
    }

    public  void clearIfExist(final int mainId)
    {
        if(find(mainId)!=-1)
        {
            put(mainId,0L);
        }
    }

    private int find(int mainId)
    {
        if(firstFree==0)
        {
            return -1;
        }

        int low = 0;
        int high = firstFree;
        int middle;
        while (low <= high && low < size)
        {
            middle = (low + high) >> 1;
            if (mainId > getInt(middle))
            {
                low = middle + 1;
            }
            else if (mainId < getInt(middle))
            {
                high = middle - 1;
            }
            else
            {
                return middle;
            }
        }
        return -1;
    }

    private int getInt(int i)
    {
        intKey.position(i*4);
        return intKey.getInt();
    }

    private long getLong(int i)
    {
        longValue.position(i * 8);
        return longValue.getLong();
    }

    public void load(FileChannel channelInt,FileChannel channelLong) throws IOException
    {
        firstFree = size;
        loadInt(channelInt);
        loadLong(channelLong);
    }
    private void loadInt(FileChannel channel) throws IOException
    {
        intKey.clear();
        long read;
        do
        {
            read = channel.read(intKey);
        }
        while (read > 0);
    }
    private void loadLong(FileChannel channel) throws IOException
    {
        longValue.clear();
        long read;
        do
        {
            read = channel.read(longValue);
        }
        while (read > 0);
    }


    public int findKeyByValueWithPrecision(long value, int skipBits) {
        longValue.position(0);
        for(int i=0;i<size;i++) {
            try
            {
                if ((longValue.getLong() >>> skipBits) == value)
                {
                    return getInt(i);
                }
            }
            catch (Exception e)
            {
                System.out.println(i + " " + e);
            }
        }
        return -1;
    }
}
