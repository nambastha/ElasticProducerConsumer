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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class OptimizedElasticProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(OptimizedElasticProducer.class);
    
    // ID Generation Strategies
    public enum IdStrategy {
        TIMESTAMP_BASED,    // Default: timestamp + random
        HASH_BASED,        // Content hash (prevents duplicates)
        EXTERNAL_ID,       // User-provided IDs
        UUID_BASED         // Pure UUID
    }
    
    private final ElasticsearchService elasticsearchService;
    private final ElasticConfig config;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService batchProcessor;
    
    private final AtomicLong recordsProduced = new AtomicLong(0);
    private final List<AuditLog> batchBuffer = new ArrayList<>();
    private final List<String> batchDocumentIds = new ArrayList<>();
    private final Set<String> seenIds = Collections.synchronizedSet(new HashSet<>());
    
    private volatile boolean running = false;
    private final Object batchLock = new Object();
    private final IdStrategy idStrategy;
    private final int maxBatchSize;
    private final int flushIntervalMs;
    
    public OptimizedElasticProducer(ElasticsearchService elasticsearchService, 
                                   ElasticConfig config, 
                                   MetricsCollector metricsCollector,
                                   IdStrategy idStrategy) {
        this.elasticsearchService = elasticsearchService;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.idStrategy = idStrategy != null ? idStrategy : IdStrategy.TIMESTAMP_BASED;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.batchProcessor = Executors.newFixedThreadPool(4);
        
        // Optimized batch configuration
        this.maxBatchSize = config.getProducerBatchSize();
        this.flushIntervalMs = config.getProducerFlushInterval();
    }
    
    /**
     * Send single event to Elasticsearch
     * @param auditLog The event to send
     * @param customId Optional custom document ID
     */
    public CompletableFuture<Boolean> sendEvent(AuditLog auditLog, String customId) {
        if (!running) {
            return CompletableFuture.completedFuture(false);
        }
        
        String documentId = generateDocumentId(auditLog, customId);
        
        // Add to batch buffer
        synchronized (batchLock) {
            batchBuffer.add(auditLog);
            batchDocumentIds.add(documentId);
            
            // Auto-flush if batch is full
            if (batchBuffer.size() >= maxBatchSize) {
                return flushBatchAsync();
            }
        }
        
        return CompletableFuture.completedFuture(true);
    }
    
    /**
     * Send batch of events to Elasticsearch
     * @param auditLogs List of events to send
     * @param customIds Optional list of custom document IDs (can be null)
     */
    public CompletableFuture<Boolean> sendEventsBatch(List<AuditLog> auditLogs, List<String> customIds) {
        if (!running || auditLogs.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        
        List<String> documentIds = new ArrayList<>();
        List<AuditLog> validEvents = new ArrayList<>();
        
        // Generate IDs and filter duplicates
        for (int i = 0; i < auditLogs.size(); i++) {
            AuditLog auditLog = auditLogs.get(i);
            String customId = (customIds != null && i < customIds.size()) ? customIds.get(i) : null;
            String documentId = generateDocumentId(auditLog, customId);
            
            // Skip duplicates if using hash-based strategy
            if (idStrategy == IdStrategy.HASH_BASED && seenIds.contains(documentId)) {
                logger.debug("Skipping duplicate event with ID: {}", documentId);
                metricsCollector.incrementDuplicatesSkipped();
                continue;
            }
            
            validEvents.add(auditLog);
            documentIds.add(documentId);
            seenIds.add(documentId);
        }
        
        if (validEvents.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        
        // Send directly for immediate processing
        return elasticsearchService.bulkIndexAuditLogs(validEvents, documentIds)
            .thenApply(success -> {
                if (success) {
                    recordsProduced.addAndGet(validEvents.size());
                    metricsCollector.incrementRecordsGenerated();
                    logger.debug("Successfully sent batch of {} events", validEvents.size());
                } else {
                    logger.error("Failed to send batch of {} events", validEvents.size());
                    metricsCollector.incrementErrors();
                }
                return success;
            });
    }
    
    /**
     * Generate document ID based on configured strategy
     */
    private String generateDocumentId(AuditLog auditLog, String customId) {
        return switch (idStrategy) {
            case EXTERNAL_ID -> {
                if (customId != null && !customId.trim().isEmpty()) {
                    yield customId;
                }
                // Fallback to timestamp if no custom ID provided
                yield generateTimestampBasedId();
            }
            
            case HASH_BASED -> generateHashBasedId(auditLog);
            
            case UUID_BASED -> UUID.randomUUID().toString();
            
            case TIMESTAMP_BASED -> generateTimestampBasedId();
        };
    }
    
    private String generateTimestampBasedId() {
        LocalDateTime now = LocalDateTime.now();
        String timestamp = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
        String random = String.format("%04d", new Random().nextInt(10000));
        return "AUDIT_" + timestamp + "_" + random;
    }
    
    private String generateHashBasedId(AuditLog auditLog) {
        try {
            // Create hash based on content (prevents duplicates)
            String content = String.format("%s|%s|%s|%s|%s", 
                auditLog.getQueryId(),
                auditLog.getUsername(), 
                auditLog.getQuery(),
                auditLog.getStartTime(),
                auditLog.getHost()
            );
            
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(content.getBytes());
            StringBuilder hexString = new StringBuilder();
            
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            
            return "HASH_" + hexString.toString().substring(0, 16); // First 16 chars
            
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Failed to generate hash-based ID, falling back to timestamp", e);
            return generateTimestampBasedId();
        }
    }
    
    public void start() {
        if (running) {
            logger.warn("Producer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting Optimized Elasticsearch producer with ID strategy: {}", idStrategy);
        
        // Schedule periodic batch flush
        scheduler.scheduleAtFixedRate(this::flushBatch, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        
        // Schedule cleanup of seen IDs (prevent memory leak)
        scheduler.scheduleAtFixedRate(this::cleanupSeenIds, 300000, 300000, TimeUnit.MILLISECONDS); // Every 5 minutes
        
        logger.info("Producer started - batch size: {}, flush interval: {} ms", maxBatchSize, flushIntervalMs);
    }
    
    public void stop() {
        if (!running) {
            logger.warn("Producer is not running");
            return;
        }
        
        logger.info("Stopping Optimized Elasticsearch producer...");
        running = false;
        
        // Flush remaining batch
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
    
    private void flushBatch() {
        synchronized (batchLock) {
            if (!batchBuffer.isEmpty()) {
                flushBatchAsync();
            }
        }
    }
    
    private CompletableFuture<Boolean> flushBatchAsync() {
        List<AuditLog> currentBatch;
        List<String> currentDocIds;
        
        synchronized (batchLock) {
            if (batchBuffer.isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }
            
            currentBatch = new ArrayList<>(batchBuffer);
            currentDocIds = new ArrayList<>(batchDocumentIds);
            
            batchBuffer.clear();
            batchDocumentIds.clear();
        }
        
        return elasticsearchService.bulkIndexAuditLogs(currentBatch, currentDocIds)
            .thenApply(success -> {
                if (success) {
                    recordsProduced.addAndGet(currentBatch.size());
                    logger.debug("Successfully flushed batch of {} records", currentBatch.size());
                    metricsCollector.recordBatchProcessed(currentBatch.size(), 0);
                } else {
                    logger.error("Failed to flush batch of {} records", currentBatch.size());
                    metricsCollector.incrementErrors();
                }
                return success;
            });
    }
    
    private void cleanupSeenIds() {
        if (seenIds.size() > 100000) { // Prevent memory leak
            synchronized (seenIds) {
                seenIds.clear();
                logger.debug("Cleared seen IDs cache to prevent memory leak");
            }
        }
    }
    
    // Monitoring methods
    public long getRecordsProduced() { return recordsProduced.get(); }
    public boolean isRunning() { return running; }
    public int getCurrentBatchSize() { 
        synchronized (batchLock) {
            return batchBuffer.size();
        }
    }
    public int getSeenIdsCount() { return seenIds.size(); }
    public IdStrategy getIdStrategy() { return idStrategy; }
}