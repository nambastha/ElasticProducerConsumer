package com.example.service;

import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.model.ConsumerOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DualIndexConsumerManager {
    
    private static final Logger logger = LoggerFactory.getLogger(DualIndexConsumerManager.class);
    
    private final ElasticConsumer primaryConsumer;
    private final ElasticConsumer secondaryConsumer;
    private final boolean enableSecondaryConsumer;
    
    public DualIndexConsumerManager(ElasticsearchService elasticsearchService,
                                  ElasticConfig primaryConfig,
                                  ElasticConfig secondaryConfig,
                                  MetricsCollector metricsCollector,
                                  boolean enableSecondaryConsumer) {
        
        this.enableSecondaryConsumer = enableSecondaryConsumer;
        
        this.primaryConsumer = new ElasticConsumer(elasticsearchService, primaryConfig, metricsCollector);
        
        if (enableSecondaryConsumer) {
            this.secondaryConsumer = new ElasticConsumer(elasticsearchService, secondaryConfig, metricsCollector);
            logger.info("Dual index consumer manager initialized with primary index: {} and secondary index: {}", 
                       primaryConfig.getConsumerSourceIndex(), secondaryConfig.getConsumerSourceIndex());
        } else {
            this.secondaryConsumer = null;
            logger.info("Single index consumer manager initialized with primary index: {}", 
                       primaryConfig.getConsumerSourceIndex());
        }
    }
    
    public void start() {
        logger.info("Starting consumer manager...");
        
        primaryConsumer.start();
        logger.info("Primary consumer started");
        
        if (enableSecondaryConsumer && secondaryConsumer != null) {
            secondaryConsumer.start();
            logger.info("Secondary consumer started");
        }
        
        logger.info("Consumer manager started successfully");
    }
    
    public void stop() {
        logger.info("Stopping consumer manager...");
        
        primaryConsumer.stop();
        logger.info("Primary consumer stopped");
        
        if (enableSecondaryConsumer && secondaryConsumer != null) {
            secondaryConsumer.stop();
            logger.info("Secondary consumer stopped");
        }
        
        logger.info("Consumer manager stopped successfully");
    }
    
    public boolean isRunning() {
        boolean primaryRunning = primaryConsumer.isRunning();
        boolean secondaryRunning = !enableSecondaryConsumer || 
                                 (secondaryConsumer != null && secondaryConsumer.isRunning());
        return primaryRunning && secondaryRunning;
    }
    
    public long getTotalRecordsProcessed() {
        long total = primaryConsumer.getRecordsProcessed();
        if (enableSecondaryConsumer && secondaryConsumer != null) {
            total += secondaryConsumer.getRecordsProcessed();
        }
        return total;
    }
    
    public Map<String, Long> getRecordsProcessedByIndex() {
        Map<String, Long> result = new HashMap<>();
        result.put("primary", primaryConsumer.getRecordsProcessed());
        
        if (enableSecondaryConsumer && secondaryConsumer != null) {
            result.put("secondary", secondaryConsumer.getRecordsProcessed());
        }
        
        return result;
    }
    
    public Map<String, ConsumerOffset> getCurrentOffsets() {
        Map<String, ConsumerOffset> result = new HashMap<>();
        result.put("primary", primaryConsumer.getCurrentOffset());
        
        if (enableSecondaryConsumer && secondaryConsumer != null) {
            result.put("secondary", secondaryConsumer.getCurrentOffset());
        }
        
        return result;
    }
    
    public ElasticConsumer getPrimaryConsumer() {
        return primaryConsumer;
    }
    
    public ElasticConsumer getSecondaryConsumer() {
        return secondaryConsumer;
    }
    
    public boolean isSecondaryConsumerEnabled() {
        return enableSecondaryConsumer;
    }
}