package com.arokis.qa;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DirectCacheTest {

    static DirectCache directCache;

    @Before
    public void setUp() throws Exception {
        directCache = new DirectCache(1);
    }

    @Test
    public void testGet() throws Exception {

    }

    @Test
    public void bitOperation()
    {
        long inputPosition = 1024*1024*1024*200L;
        int inputSize = 121;
        long combined =  ((long)(inputSize)<<48 ) | inputPosition;

        assertEquals(inputPosition,(combined & 0x0000ffffffffffffL));
        assertEquals(inputSize,(combined>>48));
    }

    @Test
    public void put()
    {
        long before = directCache.getRemaining();
        byte[] tab =   {0x00,0x12,0x11,0x12,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x11,0x10,0x10,0x01,0x14,
                0x12,0x10,0x10,0x01,0x12,0x23,
                0x11

        };
        for(int i=0;i<1024*1024*24;i++)
        directCache.put(i,tab);


        long after = directCache.getRemaining();
        System.out.println(after);
        assertTrue(before > after);

    }
}