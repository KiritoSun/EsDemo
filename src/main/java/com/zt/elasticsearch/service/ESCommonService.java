/**
 * 
 */
package com.zt.elasticsearch.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zack.zeng
 * 
 *   es API 操作封装
 *
 */
@Service
@Slf4j
public class ESCommonService {

	@Autowired
	private RestHighLevelClient elasticSearchClient;
	
	@Value("${es.query.szie:10000}")
	private int querySize=3000;
	
	public SearchResponse query(String index,QueryBuilder queryBuilder) {
		
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.size(querySize);
		searchSourceBuilder.query(queryBuilder);
//		searchSourceBuilder.sort("@timestamp", SortOrder.DESC);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.debug("{}索引不存在", index);

			} else {
				log.error("查询数据异常", e1);
			}

		} catch (Exception e) {
			log.error("查询数据异常", e);
		}
		return searchResponse;
	}
	
    public long count(String index,String statsField,QueryBuilder queryBuilder) {
		
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		AggregationBuilder agg = AggregationBuilders.count("stats_field").field(statsField);
		searchSourceBuilder.query(queryBuilder).aggregation(agg);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			
			ParsedValueCount valueCount = searchResponse.getAggregations().get("stats_field");
	       
	        return valueCount.getValue();
	       
	        
		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return 0;
			
	}

	/**
	 * @param index
	 * @param statsField
	 * @param queryBuilder
	 * @return
	 */
	public Stats stats(String index,String statsField,QueryBuilder queryBuilder) {
		
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		AggregationBuilder agg = AggregationBuilders.stats("stats_field").field(statsField);
		searchSourceBuilder.query(queryBuilder).aggregation(agg);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			
			Stats result = searchResponse.getAggregations().get("stats_field");
	        log.info("总数："+result.getCount());
	        log.info("最小值："+result.getMin());
	        log.info("最大值："+result.getMax());
	        log.info("平均值："+result.getAvg());
	        log.info("和："+result.getSum());
	        
	        return result;
	        
		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return null;
			
	}
	
	/**
	 * 分组聚合，相当于 select count(distinct countField) from t group by groupByField
	 * 
	 * @param index
	 * @param groupByField
	 * @param countField
	 * @return
	 */
	public List<Map<String, Long>> distinctCountGroupBy(String index, String groupByField, String countField,
			QueryBuilder queryBuilder) {

		List<Map<String, Long>> list = new ArrayList<Map<String, Long>>();
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("terms").field(groupByField + ".keyword");
		AggregationBuilder agg = AggregationBuilders.cardinality("stats_field").field(countField + ".keyword");
		aggregation.subAggregation(agg);
		aggregation.size(500);
		searchSourceBuilder.query(queryBuilder).aggregation(aggregation);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byGameAggregation = aggregations.get("terms");
			List<? extends Terms.Bucket> buckets = byGameAggregation.getBuckets();

			for (Terms.Bucket bucket : buckets) {
				if (bucket.getKey() == null || StringUtils.isEmpty(bucket.getKey().toString())) {
					continue;
				}
				Map<String, Long> map = new HashMap<String, Long>();
				ParsedCardinality valueCount = bucket.getAggregations().get("stats_field");
				map.put(bucket.getKey().toString(), valueCount.getValue());
				list.add(map);
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return list;
	}
	
	
	/**
	 * 分组聚合，相当于 select count(countField) from t group by groupByField
	 * 
	 * @param index
	 * @param groupByField
	 * @param countField
	 * @return
	 */
	public List<Map<String, Long>> countGroupBy(String index, String groupByField, String countField,
			QueryBuilder queryBuilder) {

		List<Map<String, Long>> list = new ArrayList<Map<String, Long>>();

		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("terms").field(groupByField + ".keyword");
		AggregationBuilder agg = AggregationBuilders.count("stats_field").field(countField + ".keyword");
		aggregation.subAggregation(agg);
		aggregation.size(500);
		searchSourceBuilder.query(queryBuilder).aggregation(aggregation);
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byGameAggregation = aggregations.get("terms");
			List<? extends Terms.Bucket> buckets = byGameAggregation.getBuckets();

			for (Terms.Bucket bucket : buckets) {
				if (bucket.getKey() == null || StringUtils.isEmpty(bucket.getKey().toString())) {
					continue;
				}
				Map<String, Long> map = new HashMap<String, Long>();
				ParsedValueCount valueCount = bucket.getAggregations().get("stats_field");
				map.put(bucket.getKey().toString(), valueCount.getValue());
				list.add(map);
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return list;
	}

	
	
	
	/**
	 * 2层分组聚合，相当于 select count(countField) from t group by groupByField1,String groupByField2,
	 * 
	 * @param index
	 * @param groupByField1,groupByField2
	 * @param countField
	 * @return
	 */
	public List<Map<String, Long>> countGroupBy2(String index, String groupByField1, String groupByField2,String countField,
			QueryBuilder queryBuilder) {

		List<Map<String, Long>> list = new ArrayList<Map<String, Long>>();
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("terms").field(groupByField1 + ".keyword");
		TermsAggregationBuilder aggregation2 = AggregationBuilders.terms("stats_").field(groupByField2 + ".keyword");
		AggregationBuilder agg = AggregationBuilders.count("stats_field").field(countField + ".keyword");
		aggregation2.subAggregation(agg);
		aggregation.subAggregation(aggregation2);
		aggregation.size(1000);
		searchSourceBuilder.query(queryBuilder).aggregation(aggregation);
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byGameAggregation = aggregations.get("terms");
			List<? extends Terms.Bucket> buckets = byGameAggregation.getBuckets();

			for (Terms.Bucket bucket : buckets) {
				if (bucket.getKey() == null || StringUtils.isEmpty(bucket.getKey().toString())) {
					continue;
				}
				
				ParsedStringTerms stat = bucket.getAggregations().get("stats_");
				for (Terms.Bucket bucket2 : stat.getBuckets()) {
					if (bucket2.getKey() == null || StringUtils.isEmpty(bucket2.getKey().toString())) {
						continue;
					}
					
					Map<String, Long> map = new HashMap<String, Long>();
					ParsedValueCount valueCount = bucket2.getAggregations().get("stats_field");
					map.put(bucket2.getKey().toString(), valueCount.getValue());
					list.add(map);
				}
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return list;
	}
	
	
	/**
	 * 分组聚合，相当于 select max(countField),avg(countField) from t group by groupByField
	 * 
	 * @param index
	 * @param groupByField
	 * @param statsField
	 * @return
	 */
	public List<Map<String, Stats>> statsGroupBy(String index, String groupByField, String statsField,
			QueryBuilder queryBuilder) {

		List<Map<String, Stats>> list = new ArrayList<Map<String, Stats>>();
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("terms").field(groupByField + ".keyword");
		AggregationBuilder agg = AggregationBuilders.stats("stats_field").field(statsField);
		aggregation.subAggregation(agg);
		aggregation.size(500);
		searchSourceBuilder.query(queryBuilder).aggregation(aggregation);
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byGameAggregation = aggregations.get("terms");
			List<? extends Terms.Bucket> buckets = byGameAggregation.getBuckets();

			for (Terms.Bucket bucket : buckets) {
				if (bucket.getKey() == null || StringUtils.isEmpty(bucket.getKey().toString())) {
					continue;
				}
				Map<String, Stats> map = new HashMap<String, Stats>();
				Stats stats = bucket.getAggregations().get("stats_field");
				log.info("分组key="+bucket.getKey().toString());
				log.info("平均值="+stats.getAvg());
				log.info("最大值="+stats.getMax());
				map.put(bucket.getKey().toString(),stats);
				list.add(map);
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return list;
	}
	
	
	/**
	 * 分组聚合，相当于 select max(countField),min(countField),avg(countField) from t group by groupByField1,groupByField2
	 * 
	 * @param index
	 * @param statsField
	 * @return
	 */
	public List<Map<String, Stats>> statsGroupBy2(String index, String groupByField1, String groupByField2,String statsField,
			QueryBuilder queryBuilder) {

		List<Map<String, Stats>> list = new ArrayList<Map<String, Stats>>();
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("terms").field(groupByField1 + ".keyword");
		TermsAggregationBuilder aggregation2 = AggregationBuilders.terms("stats_").field(groupByField2 + ".keyword");
		AggregationBuilder agg = AggregationBuilders.stats("stats_field").field(statsField);
		aggregation2.subAggregation(agg);
		aggregation.subAggregation(aggregation2);
		aggregation.size(1000);
		searchSourceBuilder.query(queryBuilder).aggregation(aggregation);
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byGameAggregation = aggregations.get("terms");
			List<? extends Terms.Bucket> buckets = byGameAggregation.getBuckets();

			for (Terms.Bucket bucket : buckets) {
				if (bucket.getKey() == null || StringUtils.isEmpty(bucket.getKey().toString())) {
					continue;
				}
				
				ParsedStringTerms stat = bucket.getAggregations().get("stats_");
				for (Terms.Bucket bucket2 : stat.getBuckets()) {
					if (bucket2.getKey() == null || StringUtils.isEmpty(bucket2.getKey().toString())) {
						continue;
					}

					Map<String, Stats> map = new HashMap<String, Stats>();
					Stats stats = bucket2.getAggregations().get("stats_field");
					log.info("分组key="+bucket2.getKey().toString());
					log.info("平均值="+stats.getAvg());
					log.info("最大值="+stats.getMax());
					log.info("最小值="+stats.getMin());
					log.info("数量="+stats.getCount());
					map.put(bucket2.getKey().toString(),stats);
					list.add(map);
				}
				
				
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}

		return list;
	}
	
	
	/**  访问成功的数目，按小时统计
	 * @param hours
	 * @param index
	 * @param queryBuilder
	 * @return
	 */
	public double dateHistogramSuccessCount(int hours,String index,BoolQueryBuilder queryBuilder){
		
		queryBuilder.filter(QueryBuilders.matchPhraseQuery("isSuccess.keyword", "Y"));
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		DateHistogramAggregationBuilder agg = AggregationBuilders.dateHistogram("by_time").field("@timestamp");
		agg.dateHistogramInterval(DateHistogramInterval.HOUR);
		agg.minDocCount(0);
		
		searchSourceBuilder.query(queryBuilder).aggregation(agg);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;

		try {

			try {
				searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
			} catch (Exception e) {
				// 超时重试
				if (e instanceof SocketTimeoutException) {
					log.warn("ES连接超时重试");
					searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
				} else {
					throw e;
				}
			}

			Histogram histogram = searchResponse.getAggregations().get("by_time");
			for (Histogram.Bucket entry : histogram.getBuckets()) {
				org.joda.time.DateTime key = (org.joda.time.DateTime) entry.getKey();
				Long count = entry.getDocCount(); // Doc count
				int h=key.toDate().getHours();
				log.info(h + ",成功访问" + count + "次");
				if(hours==h) {
					return count;
				}
			}

		} catch (ElasticsearchStatusException e1) {
			if (e1.status() == RestStatus.NOT_FOUND) {
				log.error("聚合数据异常,{}索引不存在", index);

			} else {
				log.error("聚合数据异常", e1);
			}

		} catch (Exception e) {
			log.error("聚合数据异常", e);
			throw new RuntimeException(e);
		}
		
		return 0;	
	}
	
	
	

}
