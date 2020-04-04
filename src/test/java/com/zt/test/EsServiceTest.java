package com.zt.test;

import com.alibaba.fastjson.JSON;
import com.zt.domain.entity.Movie;
import com.zt.elasticsearch.service.ESCommonService;
import com.zt.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class EsServiceTest {
    @Autowired
    private ESCommonService esCommonService;

    @Test
    public void fun() {
        List<Movie> list = Lists.newArrayList();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery();
        String index = "movies";
        SearchResponse searchResponse = esCommonService.query(index, queryBuilder);
        if(searchResponse!=null) {
            SearchHit[] results = searchResponse.getHits().getHits();
            log.info("共查询到[{}]条数据,处理数据条数[{}]", results.length, searchResponse.getHits().getTotalHits());
            for (SearchHit hit : results) {
                String sourceAsString = hit.getSourceAsString();
                if (sourceAsString != null) {
                    if(JsonUtils.isJsonObject(sourceAsString)) {
                        Movie dto= JSON.parseObject(sourceAsString, Movie.class);
                        list.add(dto);
                    }
                    if(JsonUtils.isJsonArray(sourceAsString)) {
                        List<Movie> list2= JSON.parseArray(sourceAsString, Movie.class);
                        list.addAll(list2);
                    }
                }
            }
            log.info("数据list:{}", list);
        }
    }
}
