package com.arokis.qa;

import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by tomek on 02.03.15.
 */
public class DirectCache implements IDirectCache {

    private final long totalCapacity;
    private long compactFactor = 90;
    private long compactLevel;
    private final ByteBuffer[] cache;
    private final TIntLongMap idPositionSize = new TIntLongHashMap();
    private long lastFreePosition;
    private final int partitions;
    private final int partitionSize = 1024*1024*1024;

    public DirectCache(int parts)
    {
        partitions = parts;
        cache = new ByteBuffer[partitions];
        for(int i=0;i<partitions;i++)
        {
            cache[i] = ByteBuffer.allocateDirect(partitionSize);
        }
        lastFreePosition = 0;
        totalCapacity = (long) partitions * (long) partitionSize;
        compactLevel = (totalCapacity * compactFactor)/100;
    }

    @Override
    public long getRemaining()
    {
        return (( (long) partitions * (long) partitionSize) - lastFreePosition);
    }

    @Override
    public synchronized byte[] get(int id)
    {
        long positionAndSize = idPositionSize.get(id);
        return getBytes(positionAndSize);

    }

    @Override
    public synchronized void putOrUpdate(int key, byte[] record)
    {
        if(record.length>65567)
        {
            throw new IllegalArgumentException("Record size is too large to put in cache");
        }
        long position = positionDecompile(idPositionSize.get(key));
        long size = sizeDecompile(idPositionSize.get(key));
        if(size>=record.length)
        {
            putBytes(position,record);
            idPositionSize.put(key,combine(record.length,position));
        }
        else
        {
            put(key,record);
        }
    }

    @Override
    public synchronized void delete(int maindId)
    {
        idPositionSize.put(maindId,0);
    }

    public synchronized void compact() {

        TLongIntMap temporaryMap = new TLongIntHashMap(idPositionSize.values(),idPositionSize.keys());
        lastFreePosition = 0;
        long[] values = idPositionSize.values();
        Arrays.sort(values);
        for(long value : values)
        {
            put(temporaryMap.get(value),getBytes(value));
        }
    }

    private byte[] getBytes(long positionAndSize) {
        if(positionAndSize==0)
        {
            final byte[] empty = {};
            return empty;
        }
        int size = sizeDecompile(positionAndSize);
        long position = positionDecompile(positionAndSize);
        int posInPart = (int) (position%(long)partitionSize);
        int part = calculatePartition(position);
        int offset = partitionSize-posInPart;
        byte[] record = new byte[size];
        if(offset < size)
        {
            cache[part].position(posInPart);
            cache[part].get(record,0,offset);
            part++;
            cache[part].position(0);
            cache[part].get(record,offset,size-offset);
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
        if(getRemaining()<(totalCapacity-compactLevel))
        {
            compact();
            compactFactor++;
            compactLevel=(totalCapacity*compactFactor)/100;
        }
        if(record.length > getRemaining())
        {
            throw new RuntimeException("Not Enough memory to putBytes new record: " + lastFreePosition);
        }
        long combined = combine(record.length,lastFreePosition);
        idPositionSize.put(mainId,combined);
        putBytes(lastFreePosition,record);
        lastFreePosition+=record.length;

    }

    private void putBytes(long position,byte[] record) {

        int part = calculatePartition(position);
        int posInPart = (int) (position%(long)partitionSize);
        int offset = partitionSize-posInPart;
        if(offset < record.length)
        {
            {
                cache[part].position(posInPart);
                cache[part].put(record, 0, offset);
                part++;
                cache[part].position(0);
                cache[part].put(record, offset, record.length - offset);
            }
        }
        else
        {
            cache[part].position(posInPart);
            cache[part].put(record);
        }
    }





    private long combine(int inputSize,long inputPosition)
    {
        return ((long)(inputPosition)<<16 ) | inputSize;
    }

    private long positionDecompile(long combined)
    {
        return (int)(combined>>16);
    }

    private int sizeDecompile(long combined)
    {
        return (int) (combined & 0x000000000000ffffL);
    }

    private int calculatePartition(long position)
    {
        return (int) (position/(long)partitionSize);
    }



}
