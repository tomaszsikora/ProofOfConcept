package com.deltavista.data.qa;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by t.sikora on 2015-03-13.
 */
public class CacheLoadingTest
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        DirectCache cache = new DirectCache(230000000,22000000000L);


        FileChannel mainChannel = FileChannel.open(Paths.get(args[0]+"Main"));
        FileChannel intChannel = FileChannel.open(Paths.get(args[0]+"Int"));
        FileChannel longChannel = FileChannel.open(Paths.get(args[0]+"Long"));
        long start = System.currentTimeMillis();
        //for(int i=0;i<20;i++)
        cache.load(intChannel,longChannel,mainChannel);

        long stop = System.currentTimeMillis();


        System.out.println("Load time : " + (stop - start) + " ms");
        start = System.nanoTime();
        for(int i=0;i<100;i++)
        {
            cache.get(286351364);
            cache.get(535215554);
            cache.get(363095579);
            cache.get(397643408);
            cache.get(68327840);
        }
        stop = System.nanoTime();
        System.out.println("Avg get: " + (stop-start)/500);

        while(true)
        {
            Thread.sleep(1000);
            int rand = ThreadLocalRandom.current().nextInt(315,571220063);
            System.out.println("MainId: " + rand + " record size: " + cache.get(rand).length);
        }
        //cache.get());
    }
}
