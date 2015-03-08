package com.arokis.qa;


import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

    @Test
    public void synchTest() throws InterruptedException {
        final Testowa test = new Testowa();
        ForkJoinPool pool = new ForkJoinPool(8);
        for(int i=0;i<32;i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    test.get();
                    test.put();

                }
            });
        }
        Thread.sleep(32000);


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
        directCache = new DirectCache(3);
        long before = directCache.getRemaining();
        System.out.println(directCache.getRemaining());
        byte[] tab =   {0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x00,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12


        };
        System.out.println(tab.length);
        int number = 10000000;
        long startPut = System.nanoTime();
        for(int i=0;i<number;i++)
       // if(i%2==0)
            directCache.putOrUpdate(i,tab);

        long stopPut = System.nanoTime();
        System.out.println("Avg put:" + (stopPut-startPut)/number +" Throughput: "+ ((((long) number * (long)tab.length)/(1024*1024))/TimeUnit.SECONDS.convert(stopPut-startPut,TimeUnit.NANOSECONDS))+ "MB/s");
        byte[] tab2 =   {0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,0x01,0x12,0x11,0x12,
                0x12

        };
        long test = 0;

        long start = System.nanoTime();
        for(int i=0;i<number;i++)
            test+=directCache.get(i).length;
            //assertArrayEquals(tab,directCache.get(i));
        long stop = System.nanoTime();
        System.out.println("Avg get:" + (stop-start)/number+" Throughput: "+ ((((long) number * (long)tab.length)/(1024*1024))/TimeUnit.SECONDS.convert(stop-start,TimeUnit.NANOSECONDS))+ "MB/s");
        System.out.println(directCache.getRemaining());

        IntStream natural = IntStream.iterate(0,i->i+1);

        natural.limit(number+1000).filter(i -> i%7==0).parallel().forEach(i -> directCache.putOrUpdate(i, tab2));
//        for(int i=0;i<number*3;i++)
//            if(i%7==0)
//            directCache.putOrUpdate(i, tab2);


        long after = directCache.getRemaining();
        System.out.println(after);
        assertTrue(before > after);
        start = System.nanoTime();
        directCache.compact();
        stop = System.nanoTime();
        System.out.println("Compact time :" + TimeUnit.MILLISECONDS.convert(stop-start,TimeUnit.NANOSECONDS));
        System.out.println(directCache.getRemaining());
        for(int i=0;i<10000000;i++)
            if(i%7==0)
            assertArrayEquals(tab2,directCache.get(i));
            else
                assertArrayEquals(tab,directCache.get(i));


    }
    private class Testowa
    {
       synchronized public void put() {
            System.out.println("Put Start");
           try {
               Thread.sleep(1000);
           } catch (InterruptedException e) {
               e.printStackTrace();
           }
           System.out.println("Put stop");
        }
        synchronized public void get()  {
            System.out.println("Get Start");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Get stop");
        }
    }
}