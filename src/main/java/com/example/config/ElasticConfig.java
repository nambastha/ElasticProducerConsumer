package com.example.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public class ElasticConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticConfig.class);
    
    private final Config config;
    private ElasticsearchClient client;
    
    public ElasticConfig() {
        this.config = ConfigFactory.load();
        initializeClient();
    }
    
    private void initializeClient() {
        try {
            String host = config.getString("elasticsearch.host");
            int port = config.getInt("elasticsearch.port");
            String protocol = config.getString("elasticsearch.scheme");
            String username = config.getString("elasticsearch.username");
            String password = config.getString("elasticsearch.password");
            
            BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
            credsProv.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password)
            );
            
            RestClient restClient = RestClient.builder(new HttpHost(host, port, protocol))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.setDefaultCredentialsProvider(credsProv);
                    
                    if ("https".equals(protocol)) {
                        try {
                            SSLContext sslContext = SSLContext.getInstance("TLS");
                            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
                            httpClientBuilder.setSSLContext(sslContext);
                            httpClientBuilder.setSSLHostnameVerifier((hostname, session) -> true);
                        } catch (Exception e) {
                            logger.error("Failed to configure SSL", e);
                        }
                    }
                    
                    return httpClientBuilder;
                })
                .setRequestConfigCallback(requestConfigBuilder -> 
                    requestConfigBuilder
                        .setConnectTimeout(30000)
                        .setSocketTimeout(60000)
                )
                .build();
            
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            
            ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper(objectMapper)
            );
            
            this.client = new ElasticsearchClient(transport);
            
            logger.info("Elasticsearch client initialized successfully for {}:{}", host, port);
            
        } catch (Exception e) {
            logger.error("Failed to initialize Elasticsearch client", e);
            throw new RuntimeException("Failed to initialize Elasticsearch client", e);
        }
    }
    
    private static final TrustManager[] trustAllCerts = new TrustManager[] {
        new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() { return null; }
            public void checkClientTrusted(X509Certificate[] certs, String authType) { }
            public void checkServerTrusted(X509Certificate[] certs, String authType) { }
        }
    };
    
    public ElasticsearchClient getClient() {
        return client;
    }
    
    public Config getConfig() {
        return config;
    }
    
    public String getProducerIndex() {
        return config.getString("elastic.producer.index");
    }
    
    public int getProducerBatchSize() {
        return config.getInt("elastic.producer.batch-size");
    }
    
    public int getProducerFlushInterval() {
        return config.getInt("elastic.producer.flush-interval");
    }
    
    public int getProducerRecordsPerDay() {
        return config.getInt("elastic.producer.records-per-day");
    }
    
    public String getConsumerSourceIndex() {
        return config.getString("elastic.consumer.source-index");
    }
    
    public String getConsumerOffsetIndex() {
        return config.getString("elastic.consumer.offset-index");
    }
    
    public String getConsumerId() {
        return config.getString("elastic.consumer.consumer-id");
    }
    
    public int getConsumerBatchSize() {
        return config.getInt("elastic.consumer.batch-size");
    }
    
    public int getConsumerPollInterval() {
        return config.getInt("elastic.consumer.poll-interval");
    }
    
    public String getConsumerScrollTimeout() {
        return config.getString("elastic.consumer.scroll-timeout");
    }
    
    public int getHealthPort() {
        return config.getInt("health.port");
    }
    
    public boolean isMetricsEnabled() {
        return config.getBoolean("metrics.enabled");
    }
    
    public void close() {
        try {
            if (client != null) {
                client._transport().close();
                logger.info("Elasticsearch client closed successfully");
            }
        } catch (Exception e) {
            logger.error("Failed to close Elasticsearch client", e);
        }
    }
}