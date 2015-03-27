package com.deltavista.data.structures.cache;

import java.util.BitSet;

/**
 * Created by t.sikora on 2015-03-24.
 */
public class TestBit

{
    public static void main(String[] args)
    {
        long t = System.currentTimeMillis();
        BitSet bit = new BitSet(10);

        for(int i=0;i<Integer.MAX_VALUE;i++)
        {
            bit.set(i);
        }
        System.out.println(bit.length() + " " + (System.currentTimeMillis()-t));


        t = System.currentTimeMillis();
        bit = new BitSet(10);

        for(int i=0;i<Integer.MAX_VALUE;i++)
        {
            bit.set(i);
        }
        System.out.println(bit.length() + " " + (System.currentTimeMillis()-t));


    }
}
