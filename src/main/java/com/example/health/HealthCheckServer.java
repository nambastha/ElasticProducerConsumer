package com.example.health;

import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.service.ElasticConsumer;
import com.example.service.ElasticProducer;
import com.example.service.ElasticsearchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class HealthCheckServer {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthCheckServer.class);
    
    private final ElasticConfig config;
    private final ElasticsearchService elasticsearchService;
    private final MetricsCollector metricsCollector;
    private final ObjectMapper objectMapper;
    private HttpServer server;
    
    private ElasticProducer producer;
    private ElasticConsumer consumer;
    
    public HealthCheckServer(ElasticConfig config, ElasticsearchService elasticsearchService, 
                           MetricsCollector metricsCollector) {
        this.config = config;
        this.elasticsearchService = elasticsearchService;
        this.metricsCollector = metricsCollector;
        this.objectMapper = new ObjectMapper();
    }
    
    public void setProducer(ElasticProducer producer) {
        this.producer = producer;
    }
    
    public void setConsumer(ElasticConsumer consumer) {
        this.consumer = consumer;
    }
    
    public void start() throws IOException {
        int port = config.getHealthPort();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        
        server.createContext("/health", new HealthHandler());
        server.createContext("/health/liveness", new LivenessHandler());
        server.createContext("/health/readiness", new ReadinessHandler());
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/status", new StatusHandler());
        
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        
        logger.info("Health check server started on port {}", port);
    }
    
    public void stop() {
        if (server != null) {
            server.stop(5);
            logger.info("Health check server stopped");
        }
    }
    
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, Object> health = new HashMap<>();
            
            boolean isHealthy = true;
            String status = "UP";
            
            try {
                elasticsearchService.getClient().ping();
                health.put("elasticsearch", Map.of(
                    "status", "UP",
                    "host", config.getConfig().getString("elastic.host") + ":" + config.getConfig().getInt("elastic.port")
                ));
            } catch (Exception e) {
                logger.error("Elasticsearch health check failed", e);
                isHealthy = false;
                health.put("elasticsearch", Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()
                ));
            }
            
            if (producer != null) {
                health.put("producer", Map.of(
                    "status", producer.isRunning() ? "UP" : "DOWN",
                    "recordsProduced", producer.getRecordsProduced(),
                    "currentBatchSize", producer.getCurrentBatchSize()
                ));
            }
            
            if (consumer != null) {
                health.put("consumer", Map.of(
                    "status", consumer.isRunning() ? "UP" : "DOWN",
                    "recordsProcessed", consumer.getRecordsProcessed(),
                    "totalProcessedFromOffset", consumer.getTotalProcessedFromOffset()
                ));
            }
            
            if (!isHealthy) {
                status = "DOWN";
            }
            
            health.put("status", status);
            
            String response = objectMapper.writeValueAsString(health);
            
            int statusCode = isHealthy ? 200 : 503;
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private class LivenessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, Object> liveness = Map.of(
                "status", "UP",
                "timestamp", System.currentTimeMillis()
            );
            
            String response = objectMapper.writeValueAsString(liveness);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private class ReadinessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean isReady = true;
            Map<String, Object> readiness = new HashMap<>();
            
            try {
                elasticsearchService.getClient().ping();
                readiness.put("elasticsearch", "READY");
            } catch (Exception e) {
                logger.error("Elasticsearch readiness check failed", e);
                isReady = false;
                readiness.put("elasticsearch", "NOT_READY");
            }
            
            readiness.put("status", isReady ? "READY" : "NOT_READY");
            
            String response = objectMapper.writeValueAsString(readiness);
            
            int statusCode = isReady ? 200 : 503;
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, Object> metrics = Map.of(
                "recordsGenerated", metricsCollector.getRecordsGenerated(),
                "recordsProcessed", metricsCollector.getRecordsProcessed(),
                "errors", metricsCollector.getErrors(),
                "duplicatesSkipped", metricsCollector.getDuplicatesSkipped(),
                "averageBatchProcessingTime", metricsCollector.getAverageBatchProcessingTime(),
                "averageConsumerProcessingTime", metricsCollector.getAverageConsumerProcessingTime(),
                "timestamp", System.currentTimeMillis()
            );
            
            String response = objectMapper.writeValueAsString(metrics);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
    
    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, Object> status = new HashMap<>();
            
            status.put("application", "elastic-producer-consumer");
            status.put("version", "1.0-SNAPSHOT");
            status.put("uptime", System.currentTimeMillis());
            
            if (producer != null) {
                status.put("producer", Map.of(
                    "running", producer.isRunning(),
                    "recordsProduced", producer.getRecordsProduced(),
                    "currentBatchSize", producer.getCurrentBatchSize()
                ));
            }
            
            if (consumer != null) {
                status.put("consumer", Map.of(
                    "running", consumer.isRunning(),
                    "recordsProcessed", consumer.getRecordsProcessed(),
                    "currentOffset", consumer.getCurrentOffset()
                ));
            }
            
            String response = objectMapper.writeValueAsString(status);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
}