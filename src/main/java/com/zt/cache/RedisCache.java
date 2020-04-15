package com.zt.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class RedisCache implements Cache {
    private static final Logger log = LoggerFactory.getLogger(RedisCache.class);
    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public RedisCache() {
    }

    public abstract long getTimeout();

    @Override
    public void putCache(String key, Object value) {
        try {
            if (value.getClass().equals(String.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Integer.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Double.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Float.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Short.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Long.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else if (value.getClass().equals(Boolean.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), this.getTimeout(), TimeUnit.SECONDS);
            } else {
                key = "obj#" + key;
                this.redisTemplate.opsForValue().set(key, value, this.getTimeout(), TimeUnit.SECONDS);
            }

        } catch (Exception var4) {
            log.error("putCache异常,key={},value={}", new Object[]{key, value, var4});
            throw var4;
        }
    }

    @Override
    public void putCache(String key, Object value, Long second) {
        try {
            if (value.getClass().equals(String.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Integer.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Double.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Float.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Short.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Long.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else if (value.getClass().equals(Boolean.class)) {
                this.stringRedisTemplate.opsForValue().set(key, value.toString(), second, TimeUnit.SECONDS);
            } else {
                key = "obj#" + key;
                this.redisTemplate.opsForValue().set(key, value, this.getTimeout(), TimeUnit.SECONDS);
            }

        } catch (Exception var5) {
            log.error("putCache异常,key={},value={}", new Object[]{key, value, var5});
            throw var5;
        }
    }

    @Override
    public long getSize() {
        return 0L;
    }

    @Override
    public void invalidateCaches() {
    }

    @Override
    public Object getCache(String key) {
        try {
            Object stringObj = this.stringRedisTemplate.opsForValue().get(key);
            if (stringObj != null) {
                return stringObj;
            } else {
                Object obj = this.redisTemplate.opsForValue().get("obj#" + key);
                return obj != null ? obj : null;
            }
        } catch (Exception var4) {
            log.error("getCache异常,key={}", key, var4);
            throw var4;
        }
    }

    @Override
    public boolean containsKey(String key) {
        try {
            return this.redisTemplate.hasKey("obj#" + key) || this.stringRedisTemplate.hasKey(key);
        } catch (Exception var3) {
            log.error("containsKey异常,key={}", key, var3);
            return false;
        }
    }

    @Override
    public void invalidateCache(String key) {
        try {
            this.redisTemplate.delete("obj#" + key);
            this.stringRedisTemplate.delete(key);
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
