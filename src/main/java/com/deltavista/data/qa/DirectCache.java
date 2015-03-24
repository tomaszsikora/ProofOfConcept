package com.deltavista.data.qa;

import gnu.trove.TIntCollection;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created by tomek on 02.03.15.
 */
public class DirectCache implements IDirectCache {

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private Queue<Long> toCompactSpace = new PriorityBlockingQueue<Long>();
    private final long totalCapacity;
    private final ByteBuffer[] cache;
    private final DirectIntLongMap idPositionSizeMap;
    private long lastFreePosition;
    private final int partitions;
    private final int partitionSize = 1024*1024*1024;

    private TIntObjectHashMap<byte[]> insertedRecords = new TIntObjectHashMap<byte[]>(16384, 0.9F);
    private TIntSet deleted = new TIntHashSet(10, 0.9f);

    private final byte[] EMPTY = {};

    public DirectCache(int numberOfRecords, long sizeInBytes)
    {

       idPositionSizeMap = new DirectIntLongMap(numberOfRecords);

        partitions = (int)((sizeInBytes/(long)partitionSize)+1L);
        //partitions = 20;
        cache = new ByteBuffer[partitions];
        for(int i=0;i<partitions;i++)
        {
            cache[i] = ByteBuffer.allocateDirect(partitionSize);
        }
        lastFreePosition = 0;
        totalCapacity = (long) partitions * (long) partitionSize;
        executorService.scheduleWithFixedDelay(compacting,1L,1L, TimeUnit.MILLISECONDS);
    }

    private Runnable compacting = new Runnable() {
        @Override
        public void run() {
            long positionAndSize = toCompactSpace.poll();
            if(positionAndSize == 0)
                return;
            int size = sizeDecompile(positionAndSize);
            long position = positionDecompile(positionAndSize);
            if(position+size == positionDecompile(toCompactSpace.peek()))
            {
                mergeFreeSpaces(size, position);
            }
            else
            {
                moveRecord(size, position);
            }
        }
        private void mergeFreeSpaces(final int size,final long position) {
            long next = toCompactSpace.poll();
            int nextSize = sizeDecompile(next);
            toCompactSpace.add(combine(nextSize+size,position));
        }

        private void moveRecord(int size, long position) {
            if(position+size != lastFreePosition)
            {
                int mainIdToMove = idPositionSizeMap.findKeyByValueWithPrecision(position + size, 24);
                putBytesOffHeap(position,get(mainIdToMove));
            }
            else {
                lastFreePosition = position;
            }
        }


    };

    @Override
    public long getRemaining()
    {
        return (totalCapacity - lastFreePosition);
    }

    @Override
    public synchronized byte[] get(final int id)
    {
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
    public synchronized void putOrUpdate(int key, byte[] record)
    {
        if(record.length> 16777215 )
        {
            throw new IllegalArgumentException("Record is too large to put in cache: " + record.length);
        }
        long positionAndSize = idPositionSizeMap.get(key);
        long position = positionDecompile(positionAndSize);
        int size = sizeDecompile(positionAndSize);
        if(size>=record.length)
        {
            putBytesOffHeap(position, record);
            idPositionSizeMap.put(key, combine(record.length, position));
            if(size-record.length>0)
            {
                toCompactSpace.add(combine(size-record.length,position+record.length));
            }
       }
        else
        {
            if(size>0)
            {
                idPositionSizeMap.clearIfExist(key);
                toCompactSpace.add(positionAndSize);
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
            toCompactSpace.add(idPositionSizeMap.get(maindId));
        }
        deleted.add(maindId);
    }

    public synchronized boolean isDeleted(int mainId)
    {
        return deleted.contains(mainId);
    }

    public  TIntCollection removeDeleted(TIntCollection ids)
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
    public int getOffsetForId(int maindId)
    {
        if(!isDeleted(maindId) && contains(maindId))
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
