package com.deltavista.data.qa;

import junit.framework.Assert;
import junit.framework.TestCase;

public class DirectIntLongMapTest extends TestCase
{

    public void testGet() throws Exception
    {
        DirectIntLongMap map = new DirectIntLongMap(2);
        for(int i=0;i<2;i++)
        {
            map.put(i,(long) i);

        }
        for(int i=0;i<2;i++)
        {
            assertEquals((long) i, map.get(i));
        }
    }
}