package com.zt.cache.Impl;

import com.zt.cache.LocalCache;
import org.springframework.stereotype.Component;

@Component
public class TestLocalCache extends LocalCache {
    @Override
    public long getMaxSize() {
        return 300;
    }

    @Override
    public long getTimeout() {
        return 600;
    }
}
