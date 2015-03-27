package com.deltavista.data.structures.cache;


import com.deltavista.data.structures.cache.direct.DirectCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DirectCacheTest {

    static DirectCache directCache;

    //@BeforeClass
    //public static void setUp() throws Exception {
    //    directCache = new DirectCache(1);
    //}

    @Test
    public void testGet() throws Exception {

    }

//    @Test
//    public void synchTest() throws InterruptedException {
//        final Testowa test = new Testowa();
//        ForkJoinPool pool = new ForkJoinPool(8);
//        for(int i=0;i<32;i++) {
//            pool.execute(() -> {
//                test.get();
//                test.put();
//
//            });
//        }
//        Thread.sleep(32000);
//
//
//    }

    @Test
    public void bitOperation()
    {
        long inputPosition = 1024*1024*1024*200L;
        int inputSize = 532;
        long combined =  (inputPosition <<16 ) | (long)inputSize;

        assertEquals(inputSize,(combined & 0x000000000000ffffL));
        assertEquals(inputPosition,(combined>>>16));
    }

    @Test
    public void put()
    {
        int number = 12000000;
        directCache = new DirectCache(number,1000000);
        long before = directCache.getRemaining();
        System.out.println(directCache.getRemaining());
        byte[] tab =   {0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
        };
        System.out.println(tab.length);

        long startPut = System.nanoTime();
        for(int i=0;i<number+100;i++)
       // if(i%2==0)
            directCache.putOrUpdate(i,tab);

        long stopPut = System.nanoTime();
        System.out.println("Avg put:" + (stopPut-startPut)/number +" Throughput: "+ ((((long) number * (long)tab.length)/(1024*1024))/TimeUnit.MILLISECONDS.convert(stopPut-startPut,TimeUnit.NANOSECONDS))+ "MB/ms");
        byte[] tab2 =   {0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x12

        };
        long test = 0;

        System.out.println("After put "+directCache.getHeapUsage());

        long start = System.nanoTime();
        for(int i=0;i<number;i++)
            test+=directCache.get(i).length;
            //assertArrayEquals(tab,directCache.get(i));
        System.out.println("size" + test);
        long stop = System.nanoTime();
        System.out.println("Avg get:" + (stop-start)/number );
        System.out.println(directCache.getRemaining());


        long after = directCache.getRemaining();
        System.out.println(after);
        assertTrue(before > after);
        start = System.nanoTime();
        stop = System.nanoTime();
        System.out.println("Compact time :" + TimeUnit.MILLISECONDS.convert(stop-start,TimeUnit.NANOSECONDS));
        System.out.println(directCache.getRemaining());
        for(int i=0;i<number+100;i++)
            assertEquals("number " + i, tab.length, directCache.get(i).length);

        for(int i=number-1;i>=0;i--)
        {
            directCache.putOrUpdate(i,tab2);
        }

        System.out.println("After delete  "+directCache.getHeapUsage());
        byte[] Empty = {};

        for(int i=0;i<number;i++)
            assertEquals("number " + i, tab2.length, directCache.get(i).length);

        System.out.println(directCache.getRemaining());
        for(int i=0;i<number;i++)
            directCache.putOrUpdate(i,tab);


        System.out.println("After put "+directCache.getHeapUsage());

        System.out.println(directCache.getRemaining());
        for(int i=0;i<number;i++)
            assertEquals("number " + i, tab.length, directCache.get(i).length);
        System.out.println(directCache.getRemaining());
    }


    @Test
    public void putOnHeap()
    {
        DirectCache cache = new DirectCache(3,3);
        int number = 1000;
        byte[] tab1 = {0x11,0x12,0x13};
        byte[] tab2 = {0x13,0x12,0x14};
        for(int i=0;i<number;i++)
        {
            if(i%2==0)
            {
                cache.putOrUpdate(i, tab1);
                Assert.assertArrayEquals(tab1, cache.get(i));
            }
            else
            {
                cache.putOrUpdate(i, tab2);
                Assert.assertArrayEquals(tab2, cache.get(i));
            }
        }
        for(int i=0;i<number;i++)
        {
            if(i%2==0)
            {
                cache.putOrUpdate(i, tab2);
                Assert.assertArrayEquals(tab2, cache.get(i));
            }
            else
            {
                cache.putOrUpdate(i, tab1);
                Assert.assertArrayEquals(tab1, cache.get(i));
            }

        }
        byte[] EMPTY = {};
        for(int i=0;i<number;i++)
        {
            if(i%2==0)
            {
                cache.delete(i);

                Assert.assertArrayEquals(EMPTY, cache.get(i));
            }
            else
            {
                cache.putOrUpdate(i, tab1);
                Assert.assertArrayEquals(tab1, cache.get(i));
            }

        }

        for(int i=0;i<number;i++)
        {
            if(i%2==0)
            {
                cache.putOrUpdate(i, tab2);
                Assert.assertArrayEquals(tab2, cache.get(i));
            }
            else
            {
                cache.putOrUpdate(i, tab1);
                Assert.assertArrayEquals(tab1, cache.get(i));
            }

        }

    }



    @Test
    public void updateOffHeap()
    {
        DirectCache cache = new DirectCache(3, 3);
        int number = 1000;
        byte[] tab1 = { 0x11, 0x12, 0x13 };
        byte[] tab2 = { 0x13, 0x12 };
        for (int i = 0; i < number; i++)
        {
            cache.putOrUpdate(i, tab1);
            Assert.assertArrayEquals(tab1, cache.get(i));
        }
        for (int i = 0; i < number; i++)
        {
            cache.putOrUpdate(i, tab2);
            Assert.assertArrayEquals(tab2, cache.get(i));
        }
    }


}