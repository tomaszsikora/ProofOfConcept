package com.arokis.qa;


import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DirectCacheTest {

    static DirectCache directCache;

    @BeforeClass
    public static void setUp() throws Exception {
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
        System.out.println(directCache.getRemaining());
        byte[] tab =   {0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,


        };
        for(int i=0;i<10000000;i++)
       // if(i%2==0)
            directCache.put(i,tab);

        byte[] tab2 =   {0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x12

        };
        long test = 0;
        for(int i=0;i<10000000;i++)
            test+=directCache.get(i).length;
            //assertArrayEquals(tab,directCache.get(i));

        System.out.println(directCache.getRemaining());
        for(int i=0;i<10000000;i++)
            if(i%7==0)
            directCache.update(i,tab2);


        long after = directCache.getRemaining();
        System.out.println(after);
        assertTrue(before > after);

        directCache.compact();
        System.out.println(directCache.getRemaining());
        for(int i=0;i<10000000;i++)
            if(i%7==0)
            assertArrayEquals(tab2,directCache.get(i));
            else
                assertArrayEquals(tab,directCache.get(i));


    }
}