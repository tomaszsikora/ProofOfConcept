package com.deltavista.data.qa;

import java.io.IOException;

/**
 * Created by t.sikora on 2015-03-09.
 */
public class RemoteCacheClientTest
{
    public static void main(String[] args) throws IOException
    {
        RemoteCache remoteCache = new RemoteCache(args[0]);
        byte[] tab = {0x11,0x11,0x11,0x11,0x11,0x11,0x11};
        remoteCache.getRemaining();
        int number = Integer.valueOf(args[1]);

        System.out.println(remoteCache.getRemaining());
        long start = System.nanoTime();
        for(int i=0;i<number;i++) {
            remoteCache.putOrUpdate(i,tab);
        }
        long stop = System.nanoTime();
        System.out.println("Avg:" + ((stop-start)/number));


        System.out.println(remoteCache.getRemaining());
    }
}
