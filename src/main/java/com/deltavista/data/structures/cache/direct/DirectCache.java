package com.deltavista.data.structures.cache.direct;

import com.deltavista.data.structures.cache.ICache;
import com.deltavista.data.structures.cache.ICache;
import gnu.trove.TIntCollection;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by tomek on 02.03.15.
 */
public class DirectCache implements ICache
{

    private final long totalCapacity;
    private final ByteBuffer[] cache;
    private final DirectIntLongMap idPositionSizeMap;
    private long lastFreePosition;
    private final int partitions;
    private final int partitionSize = 128*1024*1024;
    private final TIntObjectHashMap<byte[]> insertedRecords = new TIntObjectHashMap<byte[]>(16384, 0.9F);
    private final TIntSet deleted = new TIntHashSet(10, 0.9f);

    private final byte[] EMPTY = {};

    public DirectCache(int numberOfRecords, long sizeInBytes)
    {

       idPositionSizeMap = new DirectIntLongMap(numberOfRecords);

        partitions = (int)((sizeInBytes/(long)partitionSize)+1L);
        cache = new ByteBuffer[partitions];
        for(int i=0;i<partitions;i++)
        {
            cache[i] = ByteBuffer.allocateDirect(partitionSize);
        }
        lastFreePosition = 0;
        totalCapacity = (long) partitions * (long) partitionSize;
    }


    @Override
    public synchronized long getRemaining()
    {
        return (totalCapacity - lastFreePosition);
    }



    @Override
    public synchronized byte[] get(final int id)
    {
        return get0(id);
    }

    private byte[] get0(final int id)
    {
        if(deleted.contains(id))
        {
            return EMPTY;
        }
        final long positionAndSize = idPositionSizeMap.get(id);
        if(positionAndSize!=0){
            return getBytes(positionAndSize);
        }
        else
        {
            byte[] record = insertedRecords.get(id);
            return record!=null ? record : EMPTY;
        }

    }
    @Override
    public synchronized void putOrUpdate(final int key, final byte[] record)
    {
        putOrUpdate0(key,record);
    }


    private void putOrUpdate0(int key, byte[] record)
    {
        if(record.length> 16777215 )
        {
            throw new IllegalArgumentException("Record is too large to put in cache: " + record.length);
        }
        if(deleted.contains(key))
        {
            deleted.remove(key);
        }
        long positionAndSize = idPositionSizeMap.get(key);
        long position = positionDecompile(positionAndSize);
        int size = sizeDecompile(positionAndSize);
        if(size>=record.length)
        {
            putBytesOffHeap(position, record);
            idPositionSizeMap.put(key, combine(record.length, position));
       }
        else
        {
            if(size>0)
            {
                idPositionSizeMap.clearIfExist(key);
            }
            put(key,record);
        }
    }

    @Override
    public synchronized void delete(final int maindId)
    {
        if(insertedRecords.containsKey(maindId))
        {
            insertedRecords.remove(maindId);
        }
        else
        {
            idPositionSizeMap.clearIfExist(maindId);
        }
        deleted.add(maindId);
    }

    public synchronized boolean isDeleted(int mainId)
    {
        return deleted.contains(mainId);
    }

    public  synchronized TIntCollection removeDeleted(TIntCollection ids)
    {
        TIntCollection res = null;
        final TIntIterator i = ids.iterator();
            while (i.hasNext())
            {
                final int id = i.next();
                if (deleted.contains(id))
                {
                    (res == null ? (res = new TIntHashSet(ids)) : res).remove(id);
                }
            }
        return res == null ? ids : res;
    }

    private byte[] getBytes(long positionAndSize) {
        if(positionAndSize==0)
        {
            return EMPTY;
        }
        final int size = sizeDecompile(positionAndSize);
        final long position = positionDecompile(positionAndSize);
        final int posInPart = (int) (position%(long)partitionSize);
        final int part = calculatePartition(position);
        final int offset = partitionSize-posInPart;
        byte[] record = new byte[size];
        if(offset < size)
        {
            cache[part].position(posInPart);
            cache[part].get(record,0,offset);

            cache[part+1].position(0);
            cache[part+1].get(record,offset,size-offset);
        }
        else
        {
            cache[part].position(posInPart);
            cache[part].get(record);
        }
        return record;
    }


    private void put(int mainId, byte[] record)
    {

            final long combined = combine(record.length, lastFreePosition);
            if(canBePutOnOffHeap(record))
            {
                idPositionSizeMap.put(mainId, combined);
                putBytesOffHeap(lastFreePosition, record);
                lastFreePosition += record.length;
            }
            else
            {
                insertedRecords.put(mainId, record);
            }

    }

    private boolean canBePutOnOffHeap(final byte[] record)
    {
        return idPositionSizeMap.remaining()>0 && record.length < getRemaining();
    }

    private void putBytesOffHeap(final long position, final byte[] record) {

        final int part = calculatePartition(position);
        final int posInPart = (int) (position%(long)partitionSize);
        final int offset = partitionSize-posInPart;
        if(offset < record.length)
        {
            {
                cache[part].position(posInPart);
                cache[part].put(record, 0, offset);
                cache[part+1].position(0);
                cache[part+1].put(record, offset, record.length - offset);
            }
        }
        else
        {
            cache[part].position(posInPart);
            cache[part].put(record);
        }
    }

    public static long combine(final int inputSize,final long inputPosition)
    {
        return (inputPosition <<24 ) | (long)inputSize;
    }

    public static long positionDecompile(final long combined)
    {
        return combined>>>24;
    }

    public static int sizeDecompile(final long combined)
    {
        return (int) (combined & 0x0000000000ffffffL);
    }

    private int calculatePartition(final long position)
    {
        return (int) (position/(long)partitionSize);
    }

    public void load(FileChannel channelInt, FileChannel channelLong, FileChannel channelRecord) throws IOException
    {
        idPositionSizeMap.load(channelInt,channelLong);
        long read;
        do
        {
            read = channelRecord.read(cache);
        }
        while (read > 0);
    }
    @Deprecated
    public synchronized int getOffsetForId(int maindId)
    {
                if(contains(maindId))
                {
                    return maindId;
                }
                return -1;
    }

    private boolean contains(int maindId)
    {
        return ( insertedRecords.containsKey(maindId) || idPositionSizeMap.get(maindId)>0);
    }

    public int getHeapUsage()
    {
        return insertedRecords.size();
    }

}
