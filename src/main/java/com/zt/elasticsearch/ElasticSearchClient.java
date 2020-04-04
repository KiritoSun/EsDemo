package com.zt.elasticsearch;

import org.apache.http.Header;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElasticSearchClient {
    private RestHighLevelClient client;

    public ElasticSearchClient(RestHighLevelClient client) {
        this.client = client;
    }

    public IndexResponse postRequest(String index, String type, String id, String jsonSource) throws Exception {
        IndexRequest request = new IndexRequest(index, type, id);
        request.source(jsonSource, XContentType.JSON);
        IndexResponse response = this.client.index(request, new Header[0]);
        return response;
    }

    public GetResponse get(String index, String type, String id) {
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse response = null;

        try {
            response = this.client.get(getRequest, new Header[0]);
        } catch (IOException var7) {
            var7.printStackTrace();
        }

        return response;
    }

    public UpdateResponse update(String index, String type, String id, String jsonSource) throws IOException {
        UpdateRequest request = new UpdateRequest(index, type, id);
        request.doc(jsonSource, XContentType.JSON);
        UpdateResponse updateResponse = this.client.update(request, new Header[0]);
        return updateResponse;
    }

    public DeleteResponse delete(String index, String type, Long id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id.toString());
        DeleteResponse response = this.client.delete(deleteRequest, new Header[0]);
        return response;
    }

    public IndexResponse postRequest(String index, String type, String jsonSource) throws Exception {
        IndexRequest request = new IndexRequest(index, type);
        request.source(jsonSource, XContentType.JSON);
        IndexResponse response = this.client.index(request, new Header[0]);
        return response;
    }

    public SearchResponse search(String index, String type, String name) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(QueryBuilders.matchQuery("name", name));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(100);
        sourceBuilder.fetchSource(new String[]{"id", "name"}, new String[0]);
        SearchRequest searchRequest = new SearchRequest(new String[]{index});
        searchRequest.types(new String[]{type});
        searchRequest.source(sourceBuilder);
        SearchResponse response = this.client.search(searchRequest, new Header[0]);
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        SearchHit[] var10 = searchHits;
        int var11 = searchHits.length;

        for(int var12 = 0; var12 < var11; ++var12) {
            SearchHit hit = var10[var12];
            System.out.println("search -> " + hit.getSourceAsString());
        }

        return response;
    }

    public SearchResponse pageQueryRequest(String keyword1, String keyword2, String startDate, String endDate, int start, int size) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(start);
        sourceBuilder.size(size);
        sourceBuilder.timeout(new TimeValue(60L, TimeUnit.SECONDS));
        MatchQueryBuilder matchbuilder = QueryBuilders.matchQuery("message", keyword1 + " " + keyword2);
        matchbuilder.operator(Operator.AND);
        RangeQueryBuilder rangbuilder = QueryBuilders.rangeQuery("date");
        if (!"".equals(startDate)) {
            rangbuilder.gte(startDate);
        }

        if (!"".equals(endDate)) {
            rangbuilder.lte(endDate);
        }

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(matchbuilder);
        boolBuilder.must(rangbuilder);
        FieldSortBuilder fsb = SortBuilders.fieldSort("date");
        fsb.order(SortOrder.DESC);
        sourceBuilder.sort(fsb);
        sourceBuilder.query(boolBuilder);
        SearchRequest searchRequest = new SearchRequest(new String[]{"request"});
        searchRequest.types(new String[]{"doc"});
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;

        try {
            response = this.client.search(searchRequest, new Header[0]);
        } catch (IOException var15) {
            var15.printStackTrace();
        }

        return response;
    }

    public SearchResponse scrollSearch(String index, String type, String name, Object text) throws IOException {
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(new String[]{index});
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(name, text));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = this.client.search(searchRequest, new Header[0]);
        String scrollId = searchResponse.getScrollId();

        for(SearchHit[] searchHits = searchResponse.getHits().getHits(); searchHits != null && searchHits.length > 0; searchHits = searchResponse.getHits().getHits()) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = this.client.searchScroll(scrollRequest, new Header[0]);
            scrollId = searchResponse.getScrollId();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = this.client.clearScroll(clearScrollRequest, new Header[0]);
        boolean succeeded = clearScrollResponse.isSucceeded();
        return searchResponse;
    }
}
