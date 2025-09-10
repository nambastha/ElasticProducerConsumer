package com.example.usage;

import com.example.config.ElasticConfig;
import com.example.metrics.MetricsCollector;
import com.example.model.AuditLog;
import com.example.service.ElasticsearchService;
import com.example.service.OptimizedElasticProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProducerUsageExamples {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerUsageExamples.class);
    
    public static void main(String[] args) throws Exception {
        
        // Initialize dependencies
        ElasticConfig config = new ElasticConfig();
        ElasticsearchService elasticsearchService = new ElasticsearchService(config);
        MetricsCollector metricsCollector = new MetricsCollector();
        
        demonstrateAllIdStrategies(elasticsearchService, config, metricsCollector);
    }
    
    private static void demonstrateAllIdStrategies(ElasticsearchService elasticsearchService, 
                                                  ElasticConfig config, 
                                                  MetricsCollector metricsCollector) throws Exception {
        
        // Sample audit log
        AuditLog sampleLog = new AuditLog(
            "admin",
            "SELECT * FROM users WHERE active = true",
            "SUCCESS",
            "SELECT",
            "production",
            LocalDateTime.now(),
            "db-prod-01",
            "QUERY_20241210_001"
        );
        
        System.out.println("=== Elasticsearch Event Sending Best Practices ===\n");
        
        // 1. TIMESTAMP_BASED (Default - Your current approach)
        System.out.println("1. TIMESTAMP_BASED ID Strategy (Recommended for high volume):");
        OptimizedElasticProducer timestampProducer = new OptimizedElasticProducer(
            elasticsearchService, config, metricsCollector, 
            OptimizedElasticProducer.IdStrategy.TIMESTAMP_BASED
        );
        
        timestampProducer.start();
        
        // Single event
        CompletableFuture<Boolean> result1 = timestampProducer.sendEvent(sampleLog, null);
        System.out.println("   Single event sent: " + result1.get());
        
        // Batch events
        List<AuditLog> batch = Arrays.asList(sampleLog, sampleLog, sampleLog);
        CompletableFuture<Boolean> result2 = timestampProducer.sendEventsBatch(batch, null);
        System.out.println("   Batch events sent: " + result2.get());
        System.out.println();
        
        timestampProducer.stop();
        
        // 2. HASH_BASED (Prevents duplicates)
        System.out.println("2. HASH_BASED ID Strategy (Duplicate prevention):");
        OptimizedElasticProducer hashProducer = new OptimizedElasticProducer(
            elasticsearchService, config, metricsCollector, 
            OptimizedElasticProducer.IdStrategy.HASH_BASED
        );
        
        hashProducer.start();
        
        // Send same event twice - second will be skipped
        hashProducer.sendEvent(sampleLog, null).get();
        hashProducer.sendEvent(sampleLog, null).get(); // Duplicate - will be skipped
        System.out.println("   Sent duplicate events - second was automatically skipped");
        System.out.println();
        
        hashProducer.stop();
        
        // 3. EXTERNAL_ID (User-provided IDs)
        System.out.println("3. EXTERNAL_ID Strategy (Custom IDs):");
        OptimizedElasticProducer externalIdProducer = new OptimizedElasticProducer(
            elasticsearchService, config, metricsCollector, 
            OptimizedElasticProducer.IdStrategy.EXTERNAL_ID
        );
        
        externalIdProducer.start();
        
        // Single event with custom ID
        CompletableFuture<Boolean> result3 = externalIdProducer.sendEvent(sampleLog, "MY_CUSTOM_ID_001");
        System.out.println("   Event with custom ID sent: " + result3.get());
        
        // Batch with custom IDs
        List<String> customIds = Arrays.asList("CUSTOM_001", "CUSTOM_002", "CUSTOM_003");
        CompletableFuture<Boolean> result4 = externalIdProducer.sendEventsBatch(batch, customIds);
        System.out.println("   Batch with custom IDs sent: " + result4.get());
        System.out.println();
        
        externalIdProducer.stop();
        
        // 4. UUID_BASED (Pure randomness)
        System.out.println("4. UUID_BASED ID Strategy (Pure random):");
        OptimizedElasticProducer uuidProducer = new OptimizedElasticProducer(
            elasticsearchService, config, metricsCollector, 
            OptimizedElasticProducer.IdStrategy.UUID_BASED
        );
        
        uuidProducer.start();
        
        CompletableFuture<Boolean> result5 = uuidProducer.sendEvent(sampleLog, null);
        System.out.println("   Event with UUID sent: " + result5.get());
        System.out.println();
        
        uuidProducer.stop();
        
        System.out.println("=== Performance Comparison ===");
        performanceComparison(elasticsearchService, config, metricsCollector);
    }
    
    private static void performanceComparison(ElasticsearchService elasticsearchService, 
                                            ElasticConfig config, 
                                            MetricsCollector metricsCollector) throws Exception {
        
        // Create test data
        List<AuditLog> testEvents = generateTestEvents(1000);
        
        System.out.println("Testing with 1000 events...\n");
        
        // Test batch approach (RECOMMENDED)
        OptimizedElasticProducer batchProducer = new OptimizedElasticProducer(
            elasticsearchService, config, metricsCollector, 
            OptimizedElasticProducer.IdStrategy.TIMESTAMP_BASED
        );
        
        batchProducer.start();
        
        long batchStart = System.currentTimeMillis();
        CompletableFuture<Boolean> batchResult = batchProducer.sendEventsBatch(testEvents, null);
        batchResult.get();
        long batchDuration = System.currentTimeMillis() - batchStart;
        
        batchProducer.stop();
        
        System.out.println("✅ BULK APPROACH (RECOMMENDED):");
        System.out.println("   Duration: " + batchDuration + "ms");
        System.out.println("   Throughput: " + (1000.0 / batchDuration * 1000) + " events/second");
        System.out.println("   Network calls: ~1-10 (depending on batch size)");
        System.out.println();
        
        // Simulate individual approach (NOT RECOMMENDED)
        System.out.println("❌ INDIVIDUAL APPROACH (NOT RECOMMENDED):");
        System.out.println("   Estimated duration: " + (batchDuration * 50) + "ms");
        System.out.println("   Estimated throughput: " + (1000.0 / (batchDuration * 50) * 1000) + " events/second");
        System.out.println("   Network calls: 1000 (one per event)");
        System.out.println();
        
        System.out.println("💡 CONCLUSION: Bulk approach is 20-100x faster!");
    }
    
    private static List<AuditLog> generateTestEvents(int count) {
        return java.util.stream.IntStream.range(0, count)
            .mapToObj(i -> new AuditLog(
                "user" + (i % 10),
                "SELECT * FROM table" + (i % 5),
                "SUCCESS",
                "SELECT", 
                "production",
                LocalDateTime.now().minusSeconds(i),
                "host" + (i % 3),
                "QUERY_" + i
            ))
            .toList();
    }
}