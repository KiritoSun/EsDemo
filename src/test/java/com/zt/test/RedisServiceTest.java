package com.zt.test;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.zt.cache.Impl.TestCache;
import com.zt.domain.entity.Movie;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RedisServiceTest {

    @Autowired
    private TestCache testCache;
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Test
    public void fun() {
        Movie movie = new Movie();
        movie.setName("jack");
        movie.setAge("18");
        movie.setSex("man");
        testCache.putCache("#EsDemo#TestCache#Movie#"+movie.getName(), JSON.toJSONString(movie));
    }

    @Test
    public void fun2() {
        String str = testCache.getCache("#EsDemo#TestCache#Movie#jack").toString();
        Movie movie = JSON.parseObject(str, Movie.class);
        System.out.println(movie);
    }

    @Test
    public void fun3() {
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
//        valueOperations.set("number", 1, 120, TimeUnit.SECONDS);
//        valueOperations.increment("number", 3.5);
//        valueOperations.increment("number", -2.1);
//        double number = (Double) valueOperations.get("number");
//        log.info("number：{}", number);

        // 统计日活跃用户
        valueOperations.setBit("day#2020/4/21", 15001, true);
        valueOperations.setBit("day#2020/4/21", 15002, true);
        valueOperations.setBit("day#2020/4/20", 15001, true);
        valueOperations.setBit("day#2020/4/20", 15003, true);
        long count = redisTemplate.execute((RedisCallback<Long>) con -> {
            con.bitOp(RedisStringCommands.BitOperation.OR, "day#stats".getBytes()
                    , "day#2020/4/21".getBytes(), "day#2020/4/20".getBytes());
            return con.bitCount("day#stats".getBytes());
        });
        System.out.println("活跃用户：" + count);
    }

    @Test
    public void fun4() {
        HashOperations<String, String, Object> hashOperations = redisTemplate.opsForHash();
//        Map<String, Object> map = Maps.newHashMap();
//        map.put("mm", 12.2);
//        map.put("kk", 11.1);
//        map.put("gg", 23.1);
//        hashOperations.putAll("test2", map);
    }

    @Test
    public void fun5() {
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        if (valueOperations.setIfAbsent("test", "hello")) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }

    @Test
    public void fun6() {
        ListOperations<String, Object> listOperations = redisTemplate.opsForList();
        listOperations.leftPush("queue1", "data1");
        listOperations.leftPush("queue1", "data2");
        listOperations.leftPush("queue1", "data3");

        redisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                return null;
            }
        });
    }

}
