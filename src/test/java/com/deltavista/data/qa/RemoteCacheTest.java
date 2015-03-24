package com.deltavista.data.qa;

import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

public class RemoteCacheTest {


    @Test
    public void testNanos()
    {
        long start = System.nanoTime();
        int iterations = 1000;
        for(int i = 0;i<iterations;i++)
        {
            Thread.yield();
        }
        long stop = System.nanoTime();
        System.out.println("avg parkNano: "+(stop-start)/iterations);

    }

//
//
//    @Test
//    public void testGet() throws Exception {
//        RemoteCache remoteCache = new RemoteCache("C:\\temp\\remote");
//        byte[] tab = {0x11,0x11,0x11,0x11,0x11,0x11,0x11};
//        Thread.sleep(100);
//        remoteCache.getRemaining();
//        long start = System.nanoTime();
//        remoteCache.getRemaining();
//        long stop = System.nanoTime();
//        System.out.println("Single:" + (stop-start));
//        System.out.println(remoteCache.getRemaining());
//        start = System.nanoTime();
//        for(int i=0;i<1000000;i++) {
//            remoteCache.putOrUpdate(i,tab);
//        //    byte[] response = remoteCache.get(i);
//         //   if(response.length!=tab.length)
//        //        System.out.println("Puff! "+response.length);
//         //   remoteCache.delete(i);
//
//            //remoteCache.putOrUpdate(i, tab);
//        }
//        stop = System.nanoTime();
//        System.out.println("Avg:" + ((stop-start)/1000000));
//
//
//        System.out.println(remoteCache.getRemaining());
//    }
}