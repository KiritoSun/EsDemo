package com.zt.cache;

import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class LocalCache implements Cache, InitializingBean {
    private static final Logger log = LoggerFactory.getLogger(LocalCache.class);
    protected com.google.common.cache.Cache<String, Object> localCache;

    public LocalCache() {
    }

    public abstract long getMaxSize();

    public abstract long getTimeout();

    @Override
    public void afterPropertiesSet() throws Exception {
        this.localCache = CacheBuilder.newBuilder().maximumSize(this.getMaxSize()).expireAfterWrite(this.getTimeout(), TimeUnit.SECONDS).build();
    }

    @Override
    public void putCache(String key, Object value) {
        try {
            this.localCache.put(key, value);
        } catch (Exception var4) {
            log.error("putCache异常,key={},value={}", new Object[]{key, value, var4});
            throw var4;
        }
    }

    @Override
    public void putCache(String key, Object value, Long milliSecond) {
        try {
            this.localCache.put(key, value);
        } catch (Exception var5) {
            log.error("putCache异常,key={},value={}", new Object[]{key, value, var5});
            throw var5;
        }
    }

    @Override
    public long getSize() {
        return this.localCache.size();
    }

    @Override
    public void invalidateCaches() {
        try {
            this.localCache.invalidateAll();
        } catch (Exception var2) {
            log.error("invalidateAll异常", var2);
            throw var2;
        }
    }

    @Override
    public Object getCache(String key) {
        try {
            return this.localCache.getIfPresent(key);
        } catch (Exception var3) {
            log.error("getCache异常,key={}", key, var3);
            return null;
        }
    }

    @Override
    public boolean containsKey(String key) {
        try {
            return this.localCache.getIfPresent(key) != null;
        } catch (Exception var3) {
            log.error("containsKey异常,key={}", key, var3);
            return false;
        }
    }

    @Override
    public void invalidateCache(String key) {
        try {
            this.localCache.invalidate(key);
        } catch (Exception var3) {
            log.error("invalidateCache异常,key={}", key, var3);
            throw var3;
        }
    }

    @Override
    public <T> Future<T> preLoad() {
        return null;
    }
}
