package com.zt.cache.Impl;

import com.zt.cache.RedisCache;
import org.springframework.stereotype.Component;

@Component
public class TestCache extends RedisCache {
    @Override
    public long getTimeout() {
        return 600;
    }
}
