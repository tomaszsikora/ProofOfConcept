package com.arokis.qa;

import org.junit.Test;

import static org.junit.Assert.*;

public class RemoteCacheTest {





    @Test
    public void testGet() throws Exception {
        RemoteCache remoteCache = new RemoteCache("C:\\temp\\remote");
        byte[] tab = {0x11,0x11,0x11,0x11,0x11,0x11,0x11};
        Thread.sleep(100);
        remoteCache.getRemaining();
        long start = System.nanoTime();
        remoteCache.getRemaining();
        long stop = System.nanoTime();
        System.out.println("Single:" + (stop-start));
        System.out.println(remoteCache.getRemaining());
        start = System.nanoTime();
        for(int i=0;i<1_000;i++) {
            remoteCache.putOrUpdate(i,tab);
        //    byte[] response = remoteCache.get(i);
         //   if(response.length!=tab.length)
        //        System.out.println("Puff! "+response.length);
         //   remoteCache.delete(i);

            //remoteCache.putOrUpdate(i, tab);
        }
        stop = System.nanoTime();
        System.out.println("Avg:" + ((stop-start)/1_000));


        System.out.println(remoteCache.getRemaining());
    }
}