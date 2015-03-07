package com.arokis.qa;

import org.junit.Test;

import static org.junit.Assert.*;

public class RemoteCacheTest {





    @Test
    public void testGet() throws Exception {
        RemoteCache remoteCache = new RemoteCache("/tmp/testRemote");
        byte[] tab = {0x11,0x1f,0x11,0x11,0x1f,0x11,0x11,0x1f,0x11,0x11,0x1f,0x11,0x11,0x1f,0x11,0x11,0x1f,0x11,0x11};
        System.out.println(remoteCache.getRemaining());
        for(int i=0;i<12345678;i++) {
            remoteCache.putOrUpdate(i,tab);

            //remoteCache.putOrUpdate(i, tab);
        }

        System.out.println(remoteCache.getRemaining());
    }
}