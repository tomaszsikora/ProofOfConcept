package com.deltavista.data.structures.cache;

import java.util.concurrent.ExecutionException;

/**
 * Created by tomek on 06.03.15.
 */
public interface ICache
{
    long getRemaining();

    byte[] get(int id);

    void putOrUpdate(int key, byte[] record);

    void delete(int maindId);
}
