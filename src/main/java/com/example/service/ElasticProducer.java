package com.example.service;

import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.model.AuditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class ElasticProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticProducer.class);
    private static final DateTimeFormatter QUERY_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter ELASTIC_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    
    private final ElasticsearchService elasticsearchService;
    private final ElasticConfig config;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService batchProcessor;
    
    private final AtomicLong recordsProduced = new AtomicLong(0);
    private final List<AuditLog> batchBuffer = new ArrayList<>();
    private final List<String> batchDocumentIds = new ArrayList<>();
    private volatile boolean running = false;
    private final Object batchLock = new Object();
    
    private static final String[] USERNAMES = {
        "admin", "analyst", "developer", "manager", "operator", "viewer", "guest"
    };
    
    private static final String[] ACTIONS = {
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "GRANT", "REVOKE"
    };
    
    private static final String[] EFFECTS = {
        "SUCCESS", "FAILED", "DENIED", "WARNING"
    };
    
    private static final String[] ENVIRONMENTS = {
        "production", "staging", "development", "testing"
    };
    
    private static final String[] HOSTS = {
        "db-prod-01", "db-prod-02", "db-stage-01", "app-server-01", "analytics-01"
    };
    
    public ElasticProducer(ElasticsearchService elasticsearchService, ElasticConfig config, 
                          MetricsCollector metricsCollector) {
        this.elasticsearchService = elasticsearchService;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.batchProcessor = Executors.newFixedThreadPool(4);
    }
    
    public void start() {
        if (running) {
            logger.warn("Producer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting Elasticsearch producer...");
        
        int recordsPerDay = config.getProducerRecordsPerDay();
        int flushInterval = config.getProducerFlushInterval();
        
        long intervalBetweenRecords = (24 * 60 * 60 * 1000L) / recordsPerDay;
        
        scheduler.scheduleAtFixedRate(this::generateAuditLog, 0, intervalBetweenRecords, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::flushBatch, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
        
        logger.info("Producer started - will generate {} records per day (interval: {} ms)", 
                   recordsPerDay, intervalBetweenRecords);
    }
    
    public void stop() {
        if (!running) {
            logger.warn("Producer is not running");
            return;
        }
        
        logger.info("Stopping Elasticsearch producer...");
        running = false;
        
        flushBatch();
        
        scheduler.shutdown();
        batchProcessor.shutdown();
        
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!batchProcessor.awaitTermination(30, TimeUnit.SECONDS)) {
                batchProcessor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
            batchProcessor.shutdownNow();
        }
        
        logger.info("Producer stopped. Total records produced: {}", recordsProduced.get());
    }
    
    private void generateAuditLog() {
        if (!running) return;
        
        try {
            AuditLog auditLog = createRandomAuditLog();
            
            // Generate COMPLETION_AUDIT_ document IDs with timestamp format: COMPLETION_AUDIT_yyyyMMddHHmmssSSS
            String documentId = generateCompletionAuditId();
            
            synchronized (batchLock) {
                batchBuffer.add(auditLog);
                batchDocumentIds.add(documentId);
                
                if (batchBuffer.size() >= config.getProducerBatchSize()) {
                    flushBatchAsync();
                }
            }
            
            metricsCollector.incrementRecordsGenerated();
            
        } catch (Exception e) {
            logger.error("Error generating audit log", e);
            metricsCollector.incrementErrors();
        }
    }
    
    private AuditLog createRandomAuditLog() {
        Random random = new Random();
        LocalDateTime now = LocalDateTime.now();
        
        String queryId = now.format(QUERY_ID_FORMATTER) + "_" + 
                        String.format("%05d", random.nextInt(100000)) + "_" +
                        generateRandomString(5);
        
        String query = generateRandomQuery(random);
        
        return new AuditLog(
            USERNAMES[random.nextInt(USERNAMES.length)],
            query,
            EFFECTS[random.nextInt(EFFECTS.length)],
            ACTIONS[random.nextInt(ACTIONS.length)],
            ENVIRONMENTS[random.nextInt(ENVIRONMENTS.length)],
            now.minusSeconds(random.nextInt(300)),
            HOSTS[random.nextInt(HOSTS.length)],
            queryId
        );
    }
    
    private String generateRandomQuery(Random random) {
        String[] tables = {"users", "orders", "products", "inventory", "transactions", "logs"};
        String[] conditions = {"status = 'active'", "created_date > '2024-01-01'", "amount > 100", "user_id = 12345"};
        
        String table = tables[random.nextInt(tables.length)];
        String action = ACTIONS[random.nextInt(ACTIONS.length)];
        
        return switch (action) {
            case "SELECT" -> String.format("SELECT * FROM %s WHERE %s LIMIT %d", 
                                         table, conditions[random.nextInt(conditions.length)], 
                                         random.nextInt(1000) + 1);
            case "INSERT" -> String.format("INSERT INTO %s (name, value) VALUES ('item_%d', %d)", 
                                         table, random.nextInt(10000), random.nextInt(1000));
            case "UPDATE" -> String.format("UPDATE %s SET status = 'updated' WHERE %s", 
                                         table, conditions[random.nextInt(conditions.length)]);
            case "DELETE" -> String.format("DELETE FROM %s WHERE %s", 
                                         table, conditions[random.nextInt(conditions.length)]);
            default -> String.format("%s %s", action, table);
        };
    }
    
    private String generateRandomString(int length) {
        return IntStream.range(0, length)
                .map(i -> 'a' + new Random().nextInt(26))
                .collect(StringBuilder::new, (sb, c) -> sb.append((char) c), StringBuilder::append)
                .toString();
    }
    
    /**
     * Generate document ID in format: COMPLETION_AUDIT_yyyyMMddHHmmssSSS
     * Example: COMPLETION_AUDIT_20241210123045123
     */
    public static String generateCompletionAuditId() {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(ELASTIC_TIMESTAMP_FORMATTER);
        return "COMPLETION_AUDIT_" + timestamp;
    }
    
    /**
     * Generate document ID with custom timestamp
     * @param dateTime Custom timestamp
     * @return Document ID in format: COMPLETION_AUDIT_yyyyMMddHHmmssSSS
     */
    public static String generateCompletionAuditId(LocalDateTime dateTime) {
        String timestamp = dateTime.format(ELASTIC_TIMESTAMP_FORMATTER);
        return "COMPLETION_AUDIT_" + timestamp;
    }
    
    private void flushBatch() {
        synchronized (batchLock) {
            if (!batchBuffer.isEmpty()) {
                flushBatchAsync();
            }
        }
    }
    
    private void flushBatchAsync() {
        List<AuditLog> currentBatch;
        List<String> currentDocIds;
        
        synchronized (batchLock) {
            if (batchBuffer.isEmpty()) return;
            
            currentBatch = new ArrayList<>(batchBuffer);
            currentDocIds = new ArrayList<>(batchDocumentIds);
            
            batchBuffer.clear();
            batchDocumentIds.clear();
        }
        
        CompletableFuture.supplyAsync(() -> {
            try {
                long startTime = System.currentTimeMillis();
                
                boolean success = elasticsearchService.bulkIndexAuditLogs(currentBatch, currentDocIds).get(30, TimeUnit.SECONDS);
                
                long duration = System.currentTimeMillis() - startTime;
                
                if (success) {
                    long produced = recordsProduced.addAndGet(currentBatch.size());
                    logger.debug("Successfully indexed batch of {} records (total: {}) in {} ms", 
                               currentBatch.size(), produced, duration);
                    metricsCollector.recordBatchProcessed(currentBatch.size(), duration);
                } else {
                    logger.error("Failed to index batch of {} records", currentBatch.size());
                    metricsCollector.incrementErrors();
                }
                
                return success;
                
            } catch (Exception e) {
                logger.error("Error during batch flush", e);
                metricsCollector.incrementErrors();
                return false;
            }
        }, batchProcessor);
    }
    
    public long getRecordsProduced() {
        return recordsProduced.get();
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public int getCurrentBatchSize() {
        synchronized (batchLock) {
            return batchBuffer.size();
        }
    }
}