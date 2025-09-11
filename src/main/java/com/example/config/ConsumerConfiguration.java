package com.example.config;

import com.example.metrics.MetricsCollector;
import com.example.service.DualIndexConsumerManager;
import com.example.service.ElasticConsumer;
import com.example.service.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfiguration {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfiguration.class);
    
    @Value("${consumer.primary.index:audit-logs}")
    private String primaryIndex;
    
    @Value("${consumer.primary.consumer-id:primary-consumer}")
    private String primaryConsumerId;
    
    @Value("${consumer.secondary.index:}")
    private String secondaryIndex;
    
    @Value("${consumer.secondary.consumer-id:secondary-consumer}")
    private String secondaryConsumerId;
    
    @Value("${consumer.secondary.enabled:false}")
    private boolean secondaryConsumerEnabled;
    
    @Bean
    @ConditionalOnProperty(name = "consumer.dual-index.enabled", havingValue = "true")
    public DualIndexConsumerManager dualIndexConsumerManager(ElasticsearchService elasticsearchService,
                                                           ElasticConfig baseConfig,
                                                           MetricsCollector metricsCollector) {
        
        ElasticConfig primaryConfig = createConfigForIndex(baseConfig, primaryIndex, primaryConsumerId);
        ElasticConfig secondaryConfig = null;
        
        if (secondaryConsumerEnabled && !secondaryIndex.isEmpty()) {
            secondaryConfig = createConfigForIndex(baseConfig, secondaryIndex, secondaryConsumerId);
        }
        
        logger.info("Creating dual index consumer manager - Primary: {}, Secondary: {} (enabled: {})", 
                   primaryIndex, secondaryIndex, secondaryConsumerEnabled);
        
        return new DualIndexConsumerManager(elasticsearchService, primaryConfig, secondaryConfig, 
                                          metricsCollector, secondaryConsumerEnabled);
    }
    
    @Bean
    @ConditionalOnProperty(name = "consumer.dual-index.enabled", havingValue = "false", matchIfMissing = true)
    public ElasticConsumer singleIndexConsumer(ElasticsearchService elasticsearchService,
                                             ElasticConfig baseConfig,
                                             MetricsCollector metricsCollector) {
        
        ElasticConfig primaryConfig = createConfigForIndex(baseConfig, primaryIndex, primaryConsumerId);
        
        logger.info("Creating single index consumer for index: {}", primaryIndex);
        
        return new ElasticConsumer(elasticsearchService, primaryConfig, metricsCollector);
    }
    
    private ElasticConfig createConfigForIndex(ElasticConfig baseConfig, String indexName, String consumerId) {
        ElasticConfig config = new ElasticConfig();
        config.setConsumerSourceIndex(indexName);
        config.setConsumerId(consumerId);
        config.setConsumerBatchSize(baseConfig.getConsumerBatchSize());
        config.setConsumerPollInterval(baseConfig.getConsumerPollInterval());
        return config;
    }
}