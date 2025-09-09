package com.example.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.Objects;

public class ConsumerOffset {
    
    @JsonProperty("consumer_id")
    private String consumerId;
    
    @JsonProperty("source_index_name")
    private String sourceIndexName;
    
    @JsonProperty("last_processed_timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime lastProcessedTimestamp;
    
    @JsonProperty("last_processed_doc_id")
    private String lastProcessedDocId;
    
    @JsonProperty("last_updated")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime lastUpdated;
    
    @JsonProperty("total_processed")
    private Long totalProcessed;
    
    @JsonProperty("query_start_timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime queryStartTimestamp;
    
    public ConsumerOffset() {}
    
    public ConsumerOffset(String consumerId, String sourceIndexName, 
                         LocalDateTime lastProcessedTimestamp, String lastProcessedDocId,
                         LocalDateTime lastUpdated, Long totalProcessed, 
                         LocalDateTime queryStartTimestamp) {
        this.consumerId = consumerId;
        this.sourceIndexName = sourceIndexName;
        this.lastProcessedTimestamp = lastProcessedTimestamp;
        this.lastProcessedDocId = lastProcessedDocId;
        this.lastUpdated = lastUpdated;
        this.totalProcessed = totalProcessed;
        this.queryStartTimestamp = queryStartTimestamp;
    }
    
    public String getConsumerId() { return consumerId; }
    public void setConsumerId(String consumerId) { this.consumerId = consumerId; }
    
    public String getSourceIndexName() { return sourceIndexName; }
    public void setSourceIndexName(String sourceIndexName) { this.sourceIndexName = sourceIndexName; }
    
    public LocalDateTime getLastProcessedTimestamp() { return lastProcessedTimestamp; }
    public void setLastProcessedTimestamp(LocalDateTime lastProcessedTimestamp) { 
        this.lastProcessedTimestamp = lastProcessedTimestamp; 
    }
    
    public String getLastProcessedDocId() { return lastProcessedDocId; }
    public void setLastProcessedDocId(String lastProcessedDocId) { 
        this.lastProcessedDocId = lastProcessedDocId; 
    }
    
    public LocalDateTime getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(LocalDateTime lastUpdated) { this.lastUpdated = lastUpdated; }
    
    public Long getTotalProcessed() { return totalProcessed; }
    public void setTotalProcessed(Long totalProcessed) { this.totalProcessed = totalProcessed; }
    
    public LocalDateTime getQueryStartTimestamp() { return queryStartTimestamp; }
    public void setQueryStartTimestamp(LocalDateTime queryStartTimestamp) { 
        this.queryStartTimestamp = queryStartTimestamp; 
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerOffset that = (ConsumerOffset) o;
        return Objects.equals(consumerId, that.consumerId) &&
               Objects.equals(sourceIndexName, that.sourceIndexName) &&
               Objects.equals(lastProcessedTimestamp, that.lastProcessedTimestamp) &&
               Objects.equals(lastProcessedDocId, that.lastProcessedDocId) &&
               Objects.equals(lastUpdated, that.lastUpdated) &&
               Objects.equals(totalProcessed, that.totalProcessed) &&
               Objects.equals(queryStartTimestamp, that.queryStartTimestamp);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(consumerId, sourceIndexName, lastProcessedTimestamp, 
                          lastProcessedDocId, lastUpdated, totalProcessed, queryStartTimestamp);
    }
    
    @Override
    public String toString() {
        return "ConsumerOffset{" +
               "consumerId='" + consumerId + '\'' +
               ", sourceIndexName='" + sourceIndexName + '\'' +
               ", lastProcessedTimestamp=" + lastProcessedTimestamp +
               ", lastProcessedDocId='" + lastProcessedDocId + '\'' +
               ", lastUpdated=" + lastUpdated +
               ", totalProcessed=" + totalProcessed +
               ", queryStartTimestamp=" + queryStartTimestamp +
               '}';
    }
}