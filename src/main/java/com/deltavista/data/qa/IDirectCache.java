package com.deltavista.data.qa;

/**
 * Created by tomek on 06.03.15.
 */
public interface IDirectCache {
    long getRemaining();

    byte[] get(int id);

    void putOrUpdate(int key, byte[] record);

    void delete(int maindId);
}
