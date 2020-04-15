package com.zt.test;

import com.alibaba.fastjson.JSON;
import com.zt.cache.Impl.TestCache;
import com.zt.domain.entity.Movie;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RedisServiceTest {

    @Autowired
    private TestCache testCache;

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

}
