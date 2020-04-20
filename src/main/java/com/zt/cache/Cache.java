package com.zt.cache;

import java.util.concurrent.Future;

public interface Cache {
    <T> T getCache(String var1);

    void putCache(String var1, Object var2, Long var3);

    void putCache(String var1, Object var2);

    boolean containsKey(String var1);

    long getSize();

    void invalidateCaches();

    void invalidateCache(String var1);

    <T> Future<T> preLoad();
}
