package com.example;

import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.service.ElasticConsumer;
import com.example.service.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticConsumerApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticConsumerApplication.class);
    
    private final ElasticConfig config;
    private final ElasticsearchService elasticsearchService;
    private final MetricsCollector metricsCollector;
    private final ElasticConsumer consumer;
    
    public ElasticConsumerApplication() {
        this.config = new ElasticConfig();
        this.elasticsearchService = new ElasticsearchService(config);
        this.metricsCollector = new MetricsCollector();
        this.consumer = new ElasticConsumer(elasticsearchService, config, metricsCollector);
        
        setupShutdownHook();
    }
    
    public void start() {
        try {
            logger.info("Starting Elasticsearch Consumer Application...");
            
            consumer.start();
            logger.info("Consumer started");
            
            logger.info("Elasticsearch Consumer Application started successfully");
            
        } catch (Exception e) {
            logger.error("Failed to start application", e);
            shutdown();
            throw new RuntimeException("Failed to start application", e);
        }
    }
    
    public void shutdown() {
        logger.info("Shutting down Elasticsearch Consumer Application...");
        
        try {
            consumer.stop();
            logger.info("Consumer stopped");
        } catch (Exception e) {
            logger.error("Error stopping consumer", e);
        }
        
        try {
            elasticsearchService.close();
            logger.info("Elasticsearch service closed");
        } catch (Exception e) {
            logger.error("Error closing Elasticsearch service", e);
        }
        
        try {
            config.close();
            logger.info("Configuration closed");
        } catch (Exception e) {
            logger.error("Error closing configuration", e);
        }
        
        logger.info("Application shutdown completed");
    }
    
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            shutdown();
        }));
    }
    
    public static void main(String[] args) {
        ElasticConsumerApplication app = new ElasticConsumerApplication();
        
        try {
            app.start();
            
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        }
    }
}