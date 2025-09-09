package com.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import com.example.config.ElasticConfig;
import com.example.model.AuditLog;
import com.example.model.ConsumerOffset;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ElasticsearchService {
    
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);
    
    private final ElasticsearchClient client;
    private final ElasticConfig config;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    
    public ElasticsearchService(ElasticConfig config) {
        this.config = config;
        this.client = config.getClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.executorService = Executors.newFixedThreadPool(10);
        
        initializeIndices();
    }
    
    private void initializeIndices() {
        try {
            createIndexIfNotExists(config.getProducerIndex(), createAuditLogMapping());
            createIndexIfNotExists(config.getConsumerOffsetIndex(), createConsumerOffsetMapping());
            logger.info("Indices initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize indices", e);
            throw new RuntimeException("Failed to initialize indices", e);
        }
    }
    
    private void createIndexIfNotExists(String indexName, TypeMapping mapping) throws IOException {
        boolean exists = client.indices().exists(ExistsRequest.of(e -> e.index(indexName))).value();
        
        if (!exists) {
            CreateIndexRequest request = CreateIndexRequest.of(c -> c
                .index(indexName)
                .mappings(mapping)
                .settings(s -> s
                    .numberOfShards("1")
                    .numberOfReplicas("1")
                    .refreshInterval(t -> t.time("1s"))
                )
            );
            
            client.indices().create(request);
            logger.info("Created index: {}", indexName);
        } else {
            logger.info("Index already exists: {}", indexName);
        }
    }
    
    private TypeMapping createAuditLogMapping() {
        return TypeMapping.of(tm -> tm
            .properties("username", Property.of(p -> p.keyword(k -> k)))
            .properties("query", Property.of(p -> p.text(t -> t)))
            .properties("effect", Property.of(p -> p.keyword(k -> k)))
            .properties("action", Property.of(p -> p.keyword(k -> k)))
            .properties("environment", Property.of(p -> p.keyword(k -> k)))
            .properties("start_time", Property.of(p -> p.date(d -> d.format("yyyy-MM-dd HH:mm:ss.SSS"))))
            .properties("host", Property.of(p -> p.keyword(k -> k)))
            .properties("query_id", Property.of(p -> p.keyword(k -> k)))
        );
    }
    
    private TypeMapping createConsumerOffsetMapping() {
        return TypeMapping.of(tm -> tm
            .properties("consumer_id", Property.of(p -> p.keyword(k -> k)))
            .properties("source_index_name", Property.of(p -> p.keyword(k -> k)))
            .properties("last_processed_timestamp", Property.of(p -> p.date(d -> d.format("yyyy-MM-dd HH:mm:ss.SSS"))))
            .properties("last_processed_doc_id", Property.of(p -> p.keyword(k -> k)))
            .properties("last_updated", Property.of(p -> p.date(d -> d.format("yyyy-MM-dd HH:mm:ss.SSS"))))
            .properties("total_processed", Property.of(p -> p.long_(l -> l)))
            .properties("query_start_timestamp", Property.of(p -> p.date(d -> d.format("yyyy-MM-dd HH:mm:ss.SSS"))))
        );
    }
    
    public CompletableFuture<Boolean> indexAuditLog(String documentId, AuditLog auditLog) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                IndexRequest<AuditLog> request = IndexRequest.of(i -> i
                    .index(config.getProducerIndex())
                    .id(documentId)
                    .document(auditLog)
                );
                
                client.index(request);
                return true;
                
            } catch (Exception e) {
                logger.error("Failed to index audit log with id: {}", documentId, e);
                return false;
            }
        }, executorService);
    }
    
    public CompletableFuture<Boolean> bulkIndexAuditLogs(List<AuditLog> auditLogs, List<String> documentIds) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
                
                for (int i = 0; i < auditLogs.size(); i++) {
                    String docId = documentIds.get(i);
                    AuditLog auditLog = auditLogs.get(i);
                    
                    bulkBuilder.operations(op -> op
                        .index(idx -> idx
                            .index(config.getProducerIndex())
                            .id(docId)
                            .document(auditLog)
                        )
                    );
                }
                
                BulkResponse response = client.bulk(bulkBuilder.build());
                
                if (response.errors()) {
                    for (BulkResponseItem item : response.items()) {
                        if (item.error() != null) {
                            logger.error("Bulk index error for doc {}: {}", 
                                item.id(), item.error().reason());
                        }
                    }
                    return false;
                }
                
                logger.debug("Successfully bulk indexed {} audit logs", auditLogs.size());
                return true;
                
            } catch (Exception e) {
                logger.error("Failed to bulk index audit logs", e);
                return false;
            }
        }, executorService);
    }
    
    public SearchResponse<AuditLog> searchAuditLogs(SearchRequest request) throws IOException {
        return client.search(request, AuditLog.class);
    }
    
    public CompletableFuture<Boolean> saveConsumerOffset(ConsumerOffset offset) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String offsetId = offset.getConsumerId() + "_" + offset.getSourceIndexName();
                
                IndexRequest<ConsumerOffset> request = IndexRequest.of(i -> i
                    .index(config.getConsumerOffsetIndex())
                    .id(offsetId)
                    .document(offset)
                );
                
                client.index(request);
                logger.debug("Consumer offset saved for consumer: {}", offset.getConsumerId());
                return true;
                
            } catch (Exception e) {
                logger.error("Failed to save consumer offset", e);
                return false;
            }
        }, executorService);
    }
    
    public ConsumerOffset getConsumerOffset(String consumerId, String sourceIndex) {
        try {
            String offsetId = consumerId + "_" + sourceIndex;
            
            var response = client.get(g -> g
                .index(config.getConsumerOffsetIndex())
                .id(offsetId), ConsumerOffset.class);
            
            if (response.found()) {
                return response.source();
            }
            
            logger.info("No existing offset found for consumer: {}, creating new offset", consumerId);
            return new ConsumerOffset(
                consumerId, 
                sourceIndex, 
                LocalDateTime.now().minusDays(1),
                null, 
                LocalDateTime.now(), 
                0L, 
                LocalDateTime.now()
            );
            
        } catch (ElasticsearchException e) {
            if (e.response().status() == 404) {
                logger.info("Offset document not found for consumer: {}, creating new offset", consumerId);
                return new ConsumerOffset(
                    consumerId, 
                    sourceIndex, 
                    LocalDateTime.now().minusDays(1),
                    null, 
                    LocalDateTime.now(), 
                    0L, 
                    LocalDateTime.now()
                );
            }
            logger.error("Failed to get consumer offset", e);
            throw new RuntimeException("Failed to get consumer offset", e);
        } catch (Exception e) {
            logger.error("Failed to get consumer offset", e);
            throw new RuntimeException("Failed to get consumer offset", e);
        }
    }
    
    public void close() {
        try {
            executorService.shutdown();
            logger.info("ElasticsearchService closed successfully");
        } catch (Exception e) {
            logger.error("Failed to close ElasticsearchService", e);
        }
    }
    
    public ElasticsearchClient getClient() {
        return client;
    }
}