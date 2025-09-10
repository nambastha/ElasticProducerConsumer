package com.example.service;

import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.model.AuditLog;
import com.example.model.ConsumerOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BatchElasticConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchElasticConsumer.class);
    private static final DateTimeFormatter ES_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 60;
    private static final int MAX_MEMORY_THRESHOLD_MB = 500; // Maximum memory for batch buffer
    
    private final ElasticsearchService elasticsearchService;
    private final S3ParquetUploader s3ParquetUploader;
    private final ElasticConfig config;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock batchLock = new ReentrantLock();
    
    // Batch processing state
    private volatile boolean running = false;
    private volatile ConsumerOffset currentOffset;
    private final List<ProcessedRecord> batchBuffer = new ArrayList<>();
    private final AtomicLong recordsProcessed = new AtomicLong(0);
    private final AtomicLong batchesUploaded = new AtomicLong(0);
    private final Set<String> processedQueryIds = new HashSet<>();
    
    // Configuration
    private final int batchSize;
    private final String s3Bucket;
    private final String s3KeyPrefix;
    
    public BatchElasticConsumer(ElasticsearchService elasticsearchService, 
                               S3Client s3Client,
                               ElasticConfig config, 
                               MetricsCollector metricsCollector) {
        this.elasticsearchService = elasticsearchService;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.scheduler = Executors.newScheduledThreadPool(3);
        
        // Initialize batch configuration
        this.batchSize = config.getConfig().hasPath("elastic.consumer.batch-upload-size") 
            ? config.getConfig().getInt("elastic.consumer.batch-upload-size") 
            : DEFAULT_BATCH_SIZE;
            
        this.s3Bucket = config.getConfig().getString("s3.bucket");
        this.s3KeyPrefix = config.getConfig().hasPath("s3.key-prefix") 
            ? config.getConfig().getString("s3.key-prefix") 
            : "audit-logs";
            
        this.s3ParquetUploader = new S3ParquetUploader(s3Client, s3Bucket, s3KeyPrefix);
        
        initializeOffset();
    }
    
    private void initializeOffset() {
        try {
            currentOffset = elasticsearchService.getConsumerOffset(
                config.getConsumerId() + "-batch", 
                config.getConsumerSourceIndex()
            );
            
            logger.info("Batch consumer offset initialized: {}", currentOffset);
            
        } catch (Exception e) {
            logger.error("Failed to initialize batch consumer offset", e);
            throw new RuntimeException("Failed to initialize batch consumer offset", e);
        }
    }
    
    public void start() {
        if (running) {
            logger.warn("Batch consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting Batch Elasticsearch consumer with batch size: {}", batchSize);
        
        // Start the main consumer loop
        scheduler.execute(this::consumerLoop);
        
        // Schedule health checks and monitoring
        scheduler.scheduleAtFixedRate(this::healthCheck, 
            HEALTH_CHECK_INTERVAL_SECONDS, 
            HEALTH_CHECK_INTERVAL_SECONDS, 
            TimeUnit.SECONDS);
            
        // Emergency batch flush on memory pressure
        scheduler.scheduleAtFixedRate(this::checkMemoryPressure, 30, 30, TimeUnit.SECONDS);
        
        logger.info("Batch consumer started successfully");
    }
    
    public void stop() {
        if (!running) {
            logger.warn("Batch consumer is not running");
            return;
        }
        
        logger.info("Stopping Batch Elasticsearch consumer...");
        running = false;
        
        // Process remaining batch
        try {
            flushRemainingBatch();
        } catch (Exception e) {
            logger.error("Error flushing remaining batch during shutdown", e);
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        logger.info("Batch consumer stopped. Total records processed: {}, Batches uploaded: {}", 
                   recordsProcessed.get(), batchesUploaded.get());
    }
    
    private void consumerLoop() {
        logger.info("Batch consumer loop started");
        int pollInterval = config.getConsumerPollInterval();
        
        while (running) {
            try {
                int processedInCycle = pollAndBatch();
                
                // Sleep if no records were processed
                if (processedInCycle == 0) {
                    Thread.sleep(pollInterval);
                }
                
            } catch (InterruptedException e) {
                logger.info("Batch consumer loop interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in batch consumer loop", e);
                metricsCollector.incrementErrors();
                try {
                    Thread.sleep(pollInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Batch consumer loop stopped");
    }
    
    private int pollAndBatch() throws Exception {
        if (!running) return 0;
        
        long startTime = System.currentTimeMillis();
        
        SearchRequest searchRequest = buildSearchRequest();
        SearchResponse<AuditLog> response;
        
        try {
            response = elasticsearchService.searchAuditLogs(searchRequest);
        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains("no such index") || 
                                         e.getMessage().contains("all shards failed"))) {
                logger.warn("Source index '{}' not available, waiting...", config.getConsumerSourceIndex());
                return 0;
            }
            throw e;
        }
        
        List<Hit<AuditLog>> hits = response.hits().hits();
        
        if (hits.isEmpty()) {
            logger.debug("No new records found");
            return 0;
        }
        
        logger.debug("Found {} records to batch", hits.size());
        
        int processedCount = 0;
        LocalDateTime latestTimestamp = currentOffset.getLastProcessedTimestamp();
        String latestDocId = currentOffset.getLastProcessedDocId();
        
        batchLock.lock();
        try {
            for (Hit<AuditLog> hit : hits) {
                String docId = hit.id();
                AuditLog auditLog = hit.source();
                
                if (auditLog == null || !isValidRecord(docId, auditLog)) {
                    continue;
                }
                
                // Add to batch buffer
                batchBuffer.add(new ProcessedRecord(docId, auditLog));
                processedCount++;
                recordsProcessed.incrementAndGet();
                processedQueryIds.add(auditLog.getQueryId());
                
                // Update latest timestamp for offset tracking
                LocalDateTime recordTimestamp = auditLog.getStartTime();
                if (recordTimestamp != null && recordTimestamp.isAfter(latestTimestamp)) {
                    latestTimestamp = recordTimestamp;
                    latestDocId = docId;
                }
                
                // Check if batch is ready for upload
                if (batchBuffer.size() >= batchSize) {
                    logger.info("Batch size reached ({}), uploading to S3...", batchSize);
                    processBatch(latestTimestamp, latestDocId);
                }
            }
            
            // Update in-memory offset (but don't persist until batch upload)
            if (processedCount > 0) {
                updateInMemoryOffset(latestTimestamp, latestDocId, processedCount);
            }
            
        } finally {
            batchLock.unlock();
        }
        
        long duration = System.currentTimeMillis() - startTime;
        metricsCollector.recordConsumerProcessingTime(duration);
        
        logger.debug("Polling cycle completed - Batched: {}, Buffer size: {}, Duration: {} ms", 
                   processedCount, batchBuffer.size(), duration);
        
        return processedCount;
    }
    
    private boolean isValidRecord(String docId, AuditLog auditLog) {
        if (!isValidCompletionAuditDoc(docId)) {
            logger.debug("Skipping non-COMPLETION_AUDIT document: {}", docId);
            return false;
        }
        
        if (isDuplicate(auditLog.getQueryId())) {
            logger.debug("Skipping duplicate query_id: {}", auditLog.getQueryId());
            metricsCollector.incrementDuplicatesSkipped();
            return false;
        }
        
        return true;
    }
    
    private void processBatch(LocalDateTime latestTimestamp, String latestDocId) throws Exception {
        if (batchBuffer.isEmpty()) return;
        
        long startTime = System.currentTimeMillis();
        logger.info("Processing batch of {} records", batchBuffer.size());
        
        try {
            // Upload batch to S3 as parquet
            String s3Key = s3ParquetUploader.uploadBatch(new ArrayList<>(batchBuffer), 
                                                        generateBatchId());
            
            // Only commit offset after successful S3 upload
            persistOffset();
            
            batchesUploaded.incrementAndGet();
            metricsCollector.incrementBatchesProcessed();
            
            logger.info("Batch successfully uploaded to S3: {} (size: {}, duration: {} ms)", 
                       s3Key, batchBuffer.size(), System.currentTimeMillis() - startTime);
            
            // Clear batch buffer after successful upload
            batchBuffer.clear();
            
            // Clean up processed query IDs if memory gets too large
            if (processedQueryIds.size() > 50000) {
                processedQueryIds.clear();
                logger.debug("Cleared processed query IDs cache");
            }
            
        } catch (Exception e) {
            logger.error("Failed to process batch, will retry", e);
            metricsCollector.incrementErrors();
            throw e; // Don't clear buffer on failure - retry later
        }
    }
    
    private void flushRemainingBatch() throws Exception {
        batchLock.lock();
        try {
            if (!batchBuffer.isEmpty()) {
                logger.info("Flushing remaining batch of {} records", batchBuffer.size());
                processBatch(currentOffset.getLastProcessedTimestamp(), 
                            currentOffset.getLastProcessedDocId());
            }
        } finally {
            batchLock.unlock();
        }
    }
    
    private void checkMemoryPressure() {
        try {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
            long batchMemory = estimateBatchMemoryUsage();
            
            if (batchMemory > MAX_MEMORY_THRESHOLD_MB) {
                logger.warn("Memory pressure detected (batch using ~{} MB), forcing batch flush", 
                           batchMemory);
                           
                batchLock.lock();
                try {
                    if (!batchBuffer.isEmpty()) {
                        processBatch(currentOffset.getLastProcessedTimestamp(), 
                                    currentOffset.getLastProcessedDocId());
                    }
                } finally {
                    batchLock.unlock();
                }
            }
        } catch (Exception e) {
            logger.warn("Error during memory pressure check", e);
        }
    }
    
    private long estimateBatchMemoryUsage() {
        // Rough estimation: ~1KB per record
        return batchBuffer.size() / 1024;
    }
    
    private void healthCheck() {
        try {
            logger.debug("Health check - Running: {}, Buffer size: {}, Records processed: {}, Batches uploaded: {}", 
                        running, batchBuffer.size(), recordsProcessed.get(), batchesUploaded.get());
                        
            // Force batch flush if buffer has been sitting too long
            batchLock.lock();
            try {
                if (!batchBuffer.isEmpty() && shouldForceFlush()) {
                    logger.info("Forcing batch flush due to time threshold");
                    processBatch(currentOffset.getLastProcessedTimestamp(), 
                                currentOffset.getLastProcessedDocId());
                }
            } finally {
                batchLock.unlock();
            }
            
        } catch (Exception e) {
            logger.warn("Error during health check", e);
        }
    }
    
    private boolean shouldForceFlush() {
        // Force flush if batch has records older than 5 minutes
        return !batchBuffer.isEmpty() && 
               batchBuffer.get(0).getProcessedAt().isBefore(LocalDateTime.now().minusMinutes(5));
    }
    
    private SearchRequest buildSearchRequest() {
        LocalDateTime fromTime = currentOffset.getLastProcessedTimestamp();
        String lastDocId = currentOffset.getLastProcessedDocId();
        
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder()
            .must(Query.of(q -> q.range(RangeQuery.of(r -> r
                .field("start_time")
                .gte(JsonData.of(fromTime.format(ES_DATE_FORMAT)))
            ))));
        
        if (lastDocId != null && !lastDocId.trim().isEmpty()) {
            boolQueryBuilder.mustNot(Query.of(q -> q.term(t -> t
                .field("_id")
                .value(lastDocId)
            )));
        }
        
        return SearchRequest.of(s -> s
            .index(config.getConsumerSourceIndex())
            .query(Query.of(q -> q.bool(boolQueryBuilder.build())))
            .sort(sort -> sort.field(f -> f.field("start_time").order(SortOrder.Asc)))
            .size(config.getConsumerBatchSize())
            .ignoreUnavailable(true)
            .allowNoIndices(true)
        );
    }
    
    private boolean isValidCompletionAuditDoc(String docId) {
        return docId != null && docId.startsWith("COMPLETION_AUDIT_");
    }
    
    private boolean isDuplicate(String queryId) {
        return processedQueryIds.contains(queryId);
    }
    
    private void updateInMemoryOffset(LocalDateTime latestTimestamp, String latestDocId, int processedCount) {
        currentOffset.setLastProcessedTimestamp(latestTimestamp);
        currentOffset.setLastProcessedDocId(latestDocId);
        currentOffset.setLastUpdated(LocalDateTime.now());
        currentOffset.setTotalProcessed(currentOffset.getTotalProcessed() + processedCount);
    }
    
    private void persistOffset() throws Exception {
        elasticsearchService.saveConsumerOffset(currentOffset).get(30, TimeUnit.SECONDS);
        logger.debug("Batch consumer offset persisted successfully");
    }
    
    private String generateBatchId() {
        return String.format("batch_%d_%s", 
                           System.currentTimeMillis(), 
                           UUID.randomUUID().toString().substring(0, 8));
    }
    
    // Getters for monitoring
    public long getRecordsProcessed() { return recordsProcessed.get(); }
    public long getBatchesUploaded() { return batchesUploaded.get(); }
    public int getCurrentBatchSize() { return batchBuffer.size(); }
    public boolean isRunning() { return running; }
    public ConsumerOffset getCurrentOffset() { return currentOffset; }
    
    // Inner class for processed records with metadata
    public static class ProcessedRecord {
        private final String documentId;
        private final AuditLog auditLog;
        private final LocalDateTime processedAt;
        
        public ProcessedRecord(String documentId, AuditLog auditLog) {
            this.documentId = documentId;
            this.auditLog = auditLog;
            this.processedAt = LocalDateTime.now();
        }
        
        public String getDocumentId() { return documentId; }
        public AuditLog getAuditLog() { return auditLog; }
        public LocalDateTime getProcessedAt() { return processedAt; }
    }
}