package com.example.usage;

import com.example.service.ElasticProducer;
import java.time.LocalDateTime;

public class CompletionAuditIdExample {
    
    public static void main(String[] args) {
        
        System.out.println("=== COMPLETION_AUDIT ID Generation Examples ===\n");
        
        // 1. Current timestamp
        String id1 = ElasticProducer.generateCompletionAuditId();
        System.out.println("Current time ID: " + id1);
        // Output: COMPLETION_AUDIT_20241210123045123
        
        // 2. Custom timestamp
        LocalDateTime customTime = LocalDateTime.of(2024, 12, 10, 15, 30, 45, 500_000_000);
        String id2 = ElasticProducer.generateCompletionAuditId(customTime);
        System.out.println("Custom time ID:  " + id2);
        // Output: COMPLETION_AUDIT_20241210153045500
        
        // 3. Show format breakdown
        System.out.println("\n=== Format Breakdown ===");
        System.out.println("Pattern: COMPLETION_AUDIT_yyyyMMddHHmmssSSS");
        System.out.println("Example: COMPLETION_AUDIT_20241210123045123");
        System.out.println("         ^              ^      ^  ^  ^  ^");
        System.out.println("         |              |      |  |  |  |");
        System.out.println("         Prefix         Year   |  |  |  Milliseconds");
        System.out.println("                        Month--+  |  |");
        System.out.println("                        Day-------+  |");
        System.out.println("                        Hour---------+");
        System.out.println("                        Minute");
        System.out.println("                        Second");
        
        // 4. Multiple IDs (show uniqueness)
        System.out.println("\n=== Multiple IDs (showing uniqueness) ===");
        for (int i = 0; i < 5; i++) {
            String id = ElasticProducer.generateCompletionAuditId();
            System.out.println("ID " + (i+1) + ": " + id);
            
            // Small delay to ensure different milliseconds
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        System.out.println("\n✅ This format ensures:");
        System.out.println("   • Chronological ordering");
        System.out.println("   • High uniqueness (millisecond precision)");
        System.out.println("   • Easy parsing and filtering");
        System.out.println("   • Consumer compatibility (starts with COMPLETION_AUDIT_)");
    }
}