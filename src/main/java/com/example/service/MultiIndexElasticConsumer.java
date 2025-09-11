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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MultiIndexElasticConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiIndexElasticConsumer.class);
    private static final DateTimeFormatter ES_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    private final ElasticsearchService elasticsearchService;
    private final ElasticConfig config;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    private final List<String> sourceIndices;
    
    private volatile boolean running = false;
    private final Map<String, ConsumerOffset> indexOffsets = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final Map<String, Set<String>> processedQueryIdsByIndex = new ConcurrentHashMap<>();
    
    public MultiIndexElasticConsumer(ElasticsearchService elasticsearchService, 
                                   ElasticConfig config, 
                                   MetricsCollector metricsCollector,
                                   List<String> sourceIndices) {
        this.elasticsearchService = elasticsearchService;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.sourceIndices = new ArrayList<>(sourceIndices);
        this.scheduler = Executors.newScheduledThreadPool(sourceIndices.size() + 1);
        
        initializeOffsets();
    }
    
    private void initializeOffsets() {
        for (String index : sourceIndices) {
            try {
                String consumerIdForIndex = config.getConsumerId() + "_" + index;
                ConsumerOffset offset = elasticsearchService.getConsumerOffset(consumerIdForIndex, index);
                indexOffsets.put(index, offset);
                processedQueryIdsByIndex.put(index, ConcurrentHashMap.newKeySet());
                
                logger.info("Consumer offset initialized for index {}: {}", index, offset);
                
            } catch (Exception e) {
                logger.error("Failed to initialize consumer offset for index: {}", index, e);
                throw new RuntimeException("Failed to initialize consumer offset for index: " + index, e);
            }
        }
    }
    
    public void start() {
        if (running) {
            logger.warn("Multi-index consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting multi-index Elasticsearch consumer for indices: {}", sourceIndices);
        
        int pollInterval = config.getConsumerPollInterval();
        
        for (String index : sourceIndices) {
            scheduler.scheduleAtFixedRate(() -> pollAndProcessIndex(index), 0, pollInterval, TimeUnit.MILLISECONDS);
        }
        
        scheduler.scheduleAtFixedRate(this::saveAllOffsets, 30000, 30000, TimeUnit.MILLISECONDS);
        
        logger.info("Multi-index consumer started with poll interval: {} ms for {} indices", pollInterval, sourceIndices.size());
    }
    
    public void stop() {
        if (!running) {
            logger.warn("Multi-index consumer is not running");
            return;
        }
        
        logger.info("Stopping multi-index Elasticsearch consumer...");
        running = false;
        
        saveAllOffsets();
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        logger.info("Multi-index consumer stopped. Total records processed: {}", totalRecordsProcessed.get());
    }
    
    private void pollAndProcessIndex(String index) {
        if (!running) return;
        
        try {
            long startTime = System.currentTimeMillis();
            
            SearchRequest searchRequest = buildSearchRequestForIndex(index);
            SearchResponse<AuditLog> response = elasticsearchService.searchAuditLogs(searchRequest);
            
            List<Hit<AuditLog>> hits = response.hits().hits();
            
            if (hits.isEmpty()) {
                logger.debug("No new records found for index: {}", index);
                return;
            }
            
            logger.debug("Found {} records to process for index: {}", hits.size(), index);
            
            int processedCount = 0;
            int duplicatesSkipped = 0;
            ConsumerOffset currentOffset = indexOffsets.get(index);
            LocalDateTime latestTimestamp = currentOffset.getLastProcessedTimestamp();
            String latestDocId = currentOffset.getLastProcessedDocId();
            Set<String> processedQueryIds = processedQueryIdsByIndex.get(index);
            
            for (Hit<AuditLog> hit : hits) {
                String docId = hit.id();
                AuditLog auditLog = hit.source();
                
                if (auditLog == null) {
                    logger.warn("Null audit log found for document: {} in index: {}", docId, index);
                    continue;
                }
                
                if (!isValidCompletionAuditDoc(docId)) {
                    logger.debug("Skipping non-COMPLETION_AUDIT document: {} in index: {}", docId, index);
                    continue;
                }
                
                if (isDuplicate(auditLog.getQueryId(), processedQueryIds)) {
                    logger.debug("Skipping duplicate query_id: {} for document: {} in index: {}", 
                               auditLog.getQueryId(), docId, index);
                    duplicatesSkipped++;
                    metricsCollector.incrementDuplicatesSkipped();
                    continue;
                }
                
                boolean processed = processRecord(docId, auditLog, index);
                
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
                updateOffsetForIndex(index, latestTimestamp, latestDocId, processedCount);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordConsumerProcessingTime(duration);
            
            logger.debug("Processing cycle completed for index: {} - Processed: {}, Duplicates skipped: {}, Duration: {} ms", 
                       index, processedCount, duplicatesSkipped, duration);
                       
        } catch (Exception e) {
            logger.error("Error during poll and process cycle for index: {}", index, e);
            metricsCollector.incrementErrors();
        }
    }
    
    private SearchRequest buildSearchRequestForIndex(String index) {
        ConsumerOffset currentOffset = indexOffsets.get(index);
        LocalDateTime fromTime = currentOffset.getLastProcessedTimestamp();
        String lastDocId = currentOffset.getLastProcessedDocId();
        
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder()
            .must(Query.of(q -> q.range(RangeQuery.of(r -> r
                .field("start_time")
                .gte(JsonData.of(fromTime.format(ES_DATE_FORMAT)))
            ))));
        
        if (lastDocId != null) {
            boolQueryBuilder.mustNot(Query.of(q -> q.term(t -> t
                .field("_id")
                .value(lastDocId)
            )));
        }
        
        return SearchRequest.of(s -> s
            .index(index)
            .query(Query.of(q -> q.bool(boolQueryBuilder.build())))
            .sort(sort -> sort
                .field(f -> f
                    .field("start_time")
                    .order(SortOrder.Asc)
                )
            )
            .size(config.getConsumerBatchSize())
        );
    }
    
    private boolean isValidCompletionAuditDoc(String docId) {
        return docId != null && docId.startsWith("COMPLETION_AUDIT_");
    }
    
    private boolean isDuplicate(String queryId, Set<String> processedQueryIds) {
        return processedQueryIds.contains(queryId);
    }
    
    private boolean processRecord(String docId, AuditLog auditLog, String index) {
        try {
            logger.debug("Processing record - Index: {}, ID: {}, QueryID: {}, User: {}, Action: {}, Query: {}", 
                       index, docId, auditLog.getQueryId(), auditLog.getUsername(), auditLog.getAction(), 
                       auditLog.getQuery());
            
            totalRecordsProcessed.incrementAndGet();
            metricsCollector.incrementRecordsProcessed();
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to process record: {} in index: {}", docId, index, e);
            metricsCollector.incrementErrors();
            return false;
        }
    }
    
    private void updateOffsetForIndex(String index, LocalDateTime latestTimestamp, String latestDocId, int processedCount) {
        try {
            ConsumerOffset currentOffset = indexOffsets.get(index);
            currentOffset.setLastProcessedTimestamp(latestTimestamp);
            currentOffset.setLastProcessedDocId(latestDocId);
            currentOffset.setLastUpdated(LocalDateTime.now());
            currentOffset.setTotalProcessed(currentOffset.getTotalProcessed() + processedCount);
            
            logger.debug("Updated offset for index: {} - Latest timestamp: {}, Latest doc: {}, Total processed: {}", 
                       index, latestTimestamp, latestDocId, currentOffset.getTotalProcessed());
                       
        } catch (Exception e) {
            logger.error("Failed to update offset for index: {}", index, e);
            metricsCollector.incrementErrors();
        }
    }
    
    private void saveAllOffsets() {
        for (Map.Entry<String, ConsumerOffset> entry : indexOffsets.entrySet()) {
            String index = entry.getKey();
            ConsumerOffset offset = entry.getValue();
            
            if (offset != null) {
                try {
                    elasticsearchService.saveConsumerOffset(offset).get(10, TimeUnit.SECONDS);
                    logger.debug("Consumer offset saved successfully for index: {}", index);
                    
                    Set<String> processedQueryIds = processedQueryIdsByIndex.get(index);
                    if (processedQueryIds != null && processedQueryIds.size() > 10000) {
                        processedQueryIds.clear();
                        logger.debug("Cleared processed query IDs cache for index: {}", index);
                    }
                    
                } catch (Exception e) {
                    logger.error("Failed to save consumer offset for index: {}", index, e);
                    metricsCollector.incrementErrors();
                }
            }
        }
    }
    
    public long getTotalRecordsProcessed() {
        return totalRecordsProcessed.get();
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public Map<String, ConsumerOffset> getAllOffsets() {
        return new HashMap<>(indexOffsets);
    }
    
    public ConsumerOffset getOffsetForIndex(String index) {
        return indexOffsets.get(index);
    }
    
    public long getTotalProcessedForIndex(String index) {
        ConsumerOffset offset = indexOffsets.get(index);
        return offset != null ? offset.getTotalProcessed() : 0;
    }
    
    public Map<String, Long> getTotalProcessedByIndex() {
        Map<String, Long> result = new HashMap<>();
        for (Map.Entry<String, ConsumerOffset> entry : indexOffsets.entrySet()) {
            String index = entry.getKey();
            ConsumerOffset offset = entry.getValue();
            result.put(index, offset != null ? offset.getTotalProcessed() : 0L);
        }
        return result;
    }
    
    public List<String> getSourceIndices() {
        return new ArrayList<>(sourceIndices);
    }
}