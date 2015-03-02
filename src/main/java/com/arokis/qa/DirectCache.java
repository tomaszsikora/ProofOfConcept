package com.arokis.qa;

import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;

import java.nio.ByteBuffer;

/**
 * Created by tomek on 02.03.15.
 */
public class DirectCache {

    private ByteBuffer[] cache;
    private TIntLongMap idPositionSize = new TIntLongHashMap();
    private long lastFreePosition;
    private int partitions;
    private int partitionSize = Integer.MAX_VALUE;

    public DirectCache(int partitions)
    {
        this.partitions = partitions;
        cache = new ByteBuffer[partitions];
        for(int i=0;i<partitions;i++)
        {
            cache[i] = ByteBuffer.allocateDirect(partitionSize);
        }
        lastFreePosition = 0;
    }

    public long getRemaining()
    {
        return (partitions*partitionSize)-lastFreePosition;
    }

    public byte[] get(int id)
    {
        long positionAndSize = idPositionSize.get(id);
        if(positionAndSize==0)
        {
            return null;
        }
        int size = sizeDecompile(positionAndSize);
        long position = positionDecompile(positionAndSize);
        int posInPart = (int) position%partitionSize;
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


    public void put(int mainId, byte[] record)
    {
        if(record.length+lastFreePosition>partitionSize*partitions)
        {
            throw new RuntimeException("Not Enough memory to put new record");
        }
        int part = calculatePartition(lastFreePosition);
        int posInPart = (int) lastFreePosition%partitionSize;
        long combined = combine(record.length,lastFreePosition);
        lastFreePosition+=record.length;
        idPositionSize.put(mainId,combined);
        int offset = partitionSize-posInPart;
        if(offset < record.length)
        {
            {
                cache[part].position(posInPart);
                cache[part].put(record,0,offset);
                part++;
                cache[part].position(0);
                cache[part].put(record,offset,record.length-offset);
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
        return ((long)(inputSize)<<48 ) | inputPosition;
    }

    private int sizeDecompile(long combined)
    {
        return (int)(combined>>48);
    }

    private long positionDecompile(long combined)
    {
        return combined & 0x0000ffffffffffffL;
    }

    private int calculatePartition(long position)
    {
        return (int) position/partitionSize;
    }



}
