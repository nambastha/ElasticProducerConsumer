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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ElasticConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticConsumer.class);
    private static final DateTimeFormatter ES_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter ISO_INSTANT_FORMAT = DateTimeFormatter.ISO_INSTANT;
    
    private final ElasticsearchService elasticsearchService;
    private final ElasticConfig config;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    
    private volatile boolean running = false;
    private volatile ConsumerOffset currentOffset;
    private final AtomicLong recordsProcessed = new AtomicLong(0);
    private final Set<String> processedQueryIds = new HashSet<>();
    
    public ElasticConsumer(ElasticsearchService elasticsearchService, ElasticConfig config, 
                          MetricsCollector metricsCollector) {
        this.elasticsearchService = elasticsearchService;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.scheduler = Executors.newScheduledThreadPool(2);
        
        initializeOffset();
    }
    
    private void initializeOffset() {
        try {
            currentOffset = elasticsearchService.getConsumerOffset(
                config.getConsumerId(), 
                config.getConsumerSourceIndex()
            );
            
            logger.info("Consumer offset initialized: {}", currentOffset);
            
        } catch (Exception e) {
            logger.error("Failed to initialize consumer offset", e);
            throw new RuntimeException("Failed to initialize consumer offset", e);
        }
    }
    
    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting Elasticsearch consumer...");
        
        // Start the main consumer loop in a separate thread
        scheduler.execute(this::consumerLoop);
        
        // Schedule periodic offset saves
        scheduler.scheduleAtFixedRate(this::saveOffset, 30000, 30000, TimeUnit.MILLISECONDS);
        
        logger.info("Consumer started with Kafka-style continuous polling");
    }
    
    public void stop() {
        if (!running) {
            logger.warn("Consumer is not running");
            return;
        }
        
        logger.info("Stopping Elasticsearch consumer...");
        running = false;
        
        saveOffset();
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        logger.info("Consumer stopped. Total records processed: {}", recordsProcessed.get());
    }
    
    private void consumerLoop() {
        logger.info("Consumer loop started");
        int pollInterval = config.getConsumerPollInterval();
        
        while (running) {
            try {
                int processedInCycle = pollAndProcess();
                
                // Sleep only if no records were processed (like Kafka)
                if (processedInCycle == 0) {
                    Thread.sleep(pollInterval);
                }
                
            } catch (InterruptedException e) {
                logger.info("Consumer loop interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in consumer loop", e);
                try {
                    Thread.sleep(pollInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Consumer loop stopped");
    }
    
    /**
     * Converts ISO instant format (1970-01-01T00:00:00Z) to Elasticsearch format (1970-01-01 00:00:00.000)
     * @param isoInstantString ISO instant format string
     * @return Elasticsearch compatible datetime string
     */
    private String convertIsoInstantToEsFormat(String isoInstantString) {
        if (isoInstantString == null || isoInstantString.isEmpty()) {
            return null;
        }
        
        try {
            // Parse the ISO instant format and convert to UTC LocalDateTime
            Instant instant = Instant.parse(isoInstantString);
            LocalDateTime utcDateTime = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
            return utcDateTime.format(ES_DATE_FORMAT);
        } catch (Exception e) {
            logger.error("Failed to convert ISO instant '{}' to ES format", isoInstantString, e);
            return isoInstantString; // Return original if conversion fails
        }
    }
    
    private int pollAndProcess() {
        if (!running) return 0;
        
        try {
            long startTime = System.currentTimeMillis();
            
            SearchRequest searchRequest = buildSearchRequest();
            SearchResponse<AuditLog> response = null;
            
            try {
                response = elasticsearchService.searchAuditLogs(searchRequest);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("no such index")) {
                    logger.warn("Source index '{}' does not exist yet, waiting for data", config.getConsumerSourceIndex());
                    return 0;
                } else if (e.getMessage() != null && e.getMessage().contains("all shards failed")) {
                    logger.warn("All shards failed for index '{}', possibly empty or initializing", config.getConsumerSourceIndex());
                    return 0;
                } else {
                    throw e; // Re-throw other exceptions
                }
            }
            
            List<Hit<AuditLog>> hits = response.hits().hits();
            
            if (hits.isEmpty()) {
                logger.debug("No new records found");
                return 0;
            }
            
            logger.debug("Found {} records to process", hits.size());
            
            int processedCount = 0;
            int duplicatesSkipped = 0;
            LocalDateTime latestTimestamp = currentOffset.getLastProcessedTimestamp();
            String latestDocId = currentOffset.getLastProcessedDocId();
            
            for (Hit<AuditLog> hit : hits) {
                String docId = hit.id();
                AuditLog auditLog = hit.source();
                
                if (auditLog == null) {
                    logger.warn("Null audit log found for document: {}", docId);
                    continue;
                }
                
                if (!isValidCompletionAuditDoc(docId)) {
                    logger.debug("Skipping non-COMPLETION_AUDIT document: {}", docId);
                    continue;
                }
                
                if (isDuplicate(auditLog.getQueryId())) {
                    logger.debug("Skipping duplicate query_id: {} for document: {}", auditLog.getQueryId(), docId);
                    duplicatesSkipped++;
                    metricsCollector.incrementDuplicatesSkipped();
                    continue;
                }
                
                boolean processed = processRecord(docId, auditLog);
                
                if (processed) {
                    processedCount++;
                    processedQueryIds.add(auditLog.getQueryId());
                    
                    LocalDateTime recordTimestamp = auditLog.getStartTime();
                    if (recordTimestamp != null && recordTimestamp.isAfter(latestTimestamp)) {
                        latestTimestamp = recordTimestamp;
                        latestDocId = docId;
                    }
                }
            }
            
            if (processedCount > 0) {
                updateOffset(latestTimestamp, latestDocId, processedCount);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordConsumerProcessingTime(duration);
            
            logger.debug("Processing cycle completed - Processed: {}, Duplicates skipped: {}, Duration: {} ms", 
                       processedCount, duplicatesSkipped, duration);
            
            return processedCount;
                       
        } catch (Exception e) {
            logger.error("Error during poll and process cycle", e);
            metricsCollector.incrementErrors();
            return 0;
        }
    }
    
    private SearchRequest buildSearchRequest() {
        LocalDateTime fromTime = currentOffset.getLastProcessedTimestamp();
        String lastDocId = currentOffset.getLastProcessedDocId();
        
        logger.debug("Building search request - fromTime: {}, lastDocId: {}", fromTime, lastDocId);
        
        try {
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
            
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(config.getConsumerSourceIndex())
                .query(Query.of(q -> q.bool(boolQueryBuilder.build())))
                .sort(sort -> sort
                    .field(f -> f
                        .field("start_time")
                        .order(SortOrder.Asc)
                    )
                )
                .size(config.getConsumerBatchSize())
                .ignoreUnavailable(true)  // Ignore if index doesn't exist
                .allowNoIndices(true)     // Allow empty result if no indices
            );
            
            logger.debug("Search request built successfully for index: {}", config.getConsumerSourceIndex());
            return searchRequest;
            
        } catch (Exception e) {
            logger.error("Failed to build search request", e);
            throw new RuntimeException("Failed to build search request", e);
        }
    }
    
    private boolean isValidCompletionAuditDoc(String docId) {
        return docId != null && docId.startsWith("COMPLETION_AUDIT_");
    }
    
    private boolean isDuplicate(String queryId) {
        return processedQueryIds.contains(queryId);
    }
    
    private boolean processRecord(String docId, AuditLog auditLog) {
        try {
            logger.debug("Processing record - ID: {}, QueryID: {}, User: {}, Action: {}, Query: {}", 
                       docId, auditLog.getQueryId(), auditLog.getUsername(), auditLog.getAction(), 
                       auditLog.getQuery());
            
            recordsProcessed.incrementAndGet();
            metricsCollector.incrementRecordsProcessed();
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to process record: {}", docId, e);
            metricsCollector.incrementErrors();
            return false;
        }
    }
    
    private void updateOffset(LocalDateTime latestTimestamp, String latestDocId, int processedCount) {
        try {
            currentOffset.setLastProcessedTimestamp(latestTimestamp);
            currentOffset.setLastProcessedDocId(latestDocId);
            currentOffset.setLastUpdated(LocalDateTime.now());
            currentOffset.setTotalProcessed(currentOffset.getTotalProcessed() + processedCount);
            
            logger.debug("Updated offset - Latest timestamp: {}, Latest doc: {}, Total processed: {}", 
                       latestTimestamp, latestDocId, currentOffset.getTotalProcessed());
                       
        } catch (Exception e) {
            logger.error("Failed to update offset", e);
            metricsCollector.incrementErrors();
        }
    }
    
    private void saveOffset() {
        if (currentOffset != null) {
            try {
                elasticsearchService.saveConsumerOffset(currentOffset).get(10, TimeUnit.SECONDS);
                logger.debug("Consumer offset saved successfully");
                
                if (processedQueryIds.size() > 10000) {
                    processedQueryIds.clear();
                    logger.debug("Cleared processed query IDs cache");
                }
                
            } catch (Exception e) {
                logger.error("Failed to save consumer offset", e);
                metricsCollector.incrementErrors();
            }
        }
    }
    
    public long getRecordsProcessed() {
        return recordsProcessed.get();
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public ConsumerOffset getCurrentOffset() {
        return currentOffset;
    }
    
    public long getTotalProcessedFromOffset() {
        return currentOffset != null ? currentOffset.getTotalProcessed() : 0;
    }
}