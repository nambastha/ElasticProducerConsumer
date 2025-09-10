package com.example.service;

import com.example.model.AuditLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

public class S3ParquetUploader {
    
    private static final Logger logger = LoggerFactory.getLogger(S3ParquetUploader.class);
    private static final DateTimeFormatter PARQUET_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private final S3Client s3Client;
    private final String bucketName;
    private final String keyPrefix;
    private final MessageType schema;
    private final SimpleGroupFactory groupFactory;
    
    public S3ParquetUploader(S3Client s3Client, String bucketName, String keyPrefix) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.keyPrefix = keyPrefix;
        this.schema = createParquetSchema();
        this.groupFactory = new SimpleGroupFactory(schema);
    }
    
    /**
     * Uploads a batch of audit log records to S3 as a Parquet file
     * @param records List of processed records to upload
     * @param batchId Unique identifier for the batch
     * @return S3 key of the uploaded file
     * @throws Exception if upload fails
     */
    public String uploadBatch(List<BatchElasticConsumer.ProcessedRecord> records, String batchId) throws Exception {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot upload empty batch");
        }
        
        long startTime = System.currentTimeMillis();
        
        // Generate unique file names
        String fileName = generateFileName(batchId, records.size());
        String s3Key = keyPrefix + "/" + fileName;
        File tempFile = null;
        
        try {
            // Create temporary parquet file
            tempFile = createTempParquetFile(records, fileName);
            
            // Upload to S3
            uploadToS3(tempFile, s3Key);
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Successfully uploaded batch {} to S3: {} ({} records, {} bytes, {} ms)", 
                       batchId, s3Key, records.size(), tempFile.length(), duration);
            
            return s3Key;
            
        } finally {
            // Clean up temporary file
            if (tempFile != null && tempFile.exists()) {
                try {
                    Files.deleteIfExists(tempFile.toPath());
                } catch (IOException e) {
                    logger.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath(), e);
                }
            }
        }
    }
    
    private File createTempParquetFile(List<BatchElasticConsumer.ProcessedRecord> records, String fileName) throws IOException {
        File tempFile = File.createTempFile("audit_batch_", ".parquet");
        
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        
        Path filePath = new Path(tempFile.getAbsolutePath());
        
        try (ParquetWriter<Group> writer = ParquetWriter
                .<Group>builder(filePath)
                .withWriteSupport(new GroupWriteSupport())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(conf)
                .build()) {
                
            for (BatchElasticConsumer.ProcessedRecord record : records) {
                Group group = convertToParquetGroup(record);
                writer.write(group);
            }
            
            logger.debug("Created parquet file with {} records: {}", records.size(), tempFile.getAbsolutePath());
        }
        
        return tempFile;
    }
    
    private Group convertToParquetGroup(BatchElasticConsumer.ProcessedRecord record) {
        Group group = groupFactory.newGroup();
        AuditLog auditLog = record.getAuditLog();
        
        // Add fields to parquet group
        group.append("document_id", record.getDocumentId() != null ? record.getDocumentId() : "");
        group.append("query_id", auditLog.getQueryId() != null ? auditLog.getQueryId() : "");
        group.append("username", auditLog.getUsername() != null ? auditLog.getUsername() : "");
        group.append("query", auditLog.getQuery() != null ? auditLog.getQuery() : "");
        group.append("effect", auditLog.getEffect() != null ? auditLog.getEffect() : "");
        group.append("action", auditLog.getAction() != null ? auditLog.getAction() : "");
        group.append("environment", auditLog.getEnvironment() != null ? auditLog.getEnvironment() : "");
        group.append("host", auditLog.getHost() != null ? auditLog.getHost() : "");
        
        // Format timestamps as strings for parquet
        String startTime = auditLog.getStartTime() != null 
            ? auditLog.getStartTime().format(PARQUET_DATE_FORMAT) 
            : "";
        group.append("start_time", startTime);
        
        String processedAt = record.getProcessedAt() != null 
            ? record.getProcessedAt().format(PARQUET_DATE_FORMAT) 
            : "";
        group.append("processed_at", processedAt);
        
        return group;
    }
    
    private void uploadToS3(File file, String s3Key) {
        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/octet-stream")
                .metadata(java.util.Map.of(
                    "content-type", "parquet",
                    "source", "elastic-batch-consumer",
                    "upload-time", LocalDateTime.now().toString()
                ))
                .build();
                
            s3Client.putObject(putRequest, RequestBody.fromFile(file));
            
            logger.debug("File uploaded to S3: s3://{}/{}", bucketName, s3Key);
            
        } catch (Exception e) {
            logger.error("Failed to upload file to S3: s3://{}/{}", bucketName, s3Key, e);
            throw new RuntimeException("S3 upload failed", e);
        }
    }
    
    private String generateFileName(String batchId, int recordCount) {
        LocalDateTime now = LocalDateTime.now();
        return String.format("year=%d/month=%02d/day=%02d/audit_logs_%s_%d_records_%s.parquet",
            now.getYear(),
            now.getMonthValue(),
            now.getDayOfMonth(),
            batchId,
            recordCount,
            now.format(DateTimeFormatter.ofPattern("HHmmss"))
        );
    }
    
    private MessageType createParquetSchema() {
        return Types.buildMessage()
            .addFields(
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("document_id"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("query_id"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("username"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("query"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("effect"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("action"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("environment"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("host"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("start_time"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, org.apache.parquet.schema.Type.Repetition.OPTIONAL)
                    .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                    .named("processed_at")
            )
            .named("audit_log");
    }
}