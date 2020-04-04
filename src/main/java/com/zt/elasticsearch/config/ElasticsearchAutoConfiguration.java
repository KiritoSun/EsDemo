package com.zt.elasticsearch.config;

import com.zt.elasticsearch.ElasticSearchClient;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
@ConditionalOnClass({RestHighLevelClient.class})
@EnableConfigurationProperties({ElasticsearchProperties.class})
public class ElasticsearchAutoConfiguration {
    @Autowired
    private ElasticsearchProperties elasticsearchProperties;
    private static String schema = "http";
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    public ElasticsearchAutoConfiguration() {
    }

    @Bean
    @ConditionalOnProperty(
            name = {"elasticsearch.host"}
    )
    public ElasticSearchClient elasticSearchClient() {
        return new ElasticSearchClient(this.client());
    }

    @Bean
    public RestHighLevelClient client() {
        this.credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.elasticsearchProperties.getUsername(), this.elasticsearchProperties.getPassword()));
        RestClientBuilder builder = RestClient.builder(new HttpHost[]{new HttpHost(this.elasticsearchProperties.getHost(), this.elasticsearchProperties.getPort(), schema)});
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getConnectTimeOut());
                requestConfigBuilder.setSocketTimeout(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getSocketTimeOut());
                requestConfigBuilder.setConnectionRequestTimeout(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getConnectionRequestTimeOut());
                return requestConfigBuilder;
            }
        });
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.setMaxConnTotal(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getMaxConnectNum());
                httpClientBuilder.setMaxConnPerRoute(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getMaxConnectPerRoute());
                if (!StringUtils.isEmpty(ElasticsearchAutoConfiguration.this.elasticsearchProperties.getUsername())) {
                    httpClientBuilder.setDefaultCredentialsProvider(ElasticsearchAutoConfiguration.this.credentialsProvider);
                }

                return httpClientBuilder;
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
