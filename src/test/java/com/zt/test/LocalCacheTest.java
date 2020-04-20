package com.zt.test;

import com.alibaba.fastjson.JSON;
import com.zt.cache.Impl.TestLocalCache;
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
public class LocalCacheTest {
    @Autowired
    private TestLocalCache testLocalCache;

    @Test
    public void fun() {
        Movie movie = new Movie();
        movie.setName("jack");
        movie.setSex("man");
        movie.setAge("19");
        testLocalCache.putCache("#test#info", JSON.toJSONString(movie));
        String str = testLocalCache.getCache("#test#info").toString();
        try {
            Movie cacheMovie = JSON.parseObject(str, Movie.class);
            log.info("movie：{}", cacheMovie);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
