package com.example.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.Objects;

public class AuditLog {
    
    @JsonProperty("username")
    private String username;
    
    @JsonProperty("query")
    private String query;
    
    @JsonProperty("effect")
    private String effect;
    
    @JsonProperty("action")
    private String action;
    
    @JsonProperty("environment")
    private String environment;
    
    @JsonProperty("start_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    private LocalDateTime startTime;
    
    @JsonProperty("host")
    private String host;
    
    @JsonProperty("query_id")
    private String queryId;
    
    public AuditLog() {}
    
    public AuditLog(String username, String query, String effect, String action, 
                   String environment, LocalDateTime startTime, String host, String queryId) {
        this.username = username;
        this.query = query;
        this.effect = effect;
        this.action = action;
        this.environment = environment;
        this.startTime = startTime;
        this.host = host;
        this.queryId = queryId;
    }
    
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    
    public String getQuery() { return query; }
    public void setQuery(String query) { this.query = query; }
    
    public String getEffect() { return effect; }
    public void setEffect(String effect) { this.effect = effect; }
    
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    
    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }
    
    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
    
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    
    public String getQueryId() { return queryId; }
    public void setQueryId(String queryId) { this.queryId = queryId; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuditLog auditLog = (AuditLog) o;
        return Objects.equals(username, auditLog.username) &&
               Objects.equals(query, auditLog.query) &&
               Objects.equals(effect, auditLog.effect) &&
               Objects.equals(action, auditLog.action) &&
               Objects.equals(environment, auditLog.environment) &&
               Objects.equals(startTime, auditLog.startTime) &&
               Objects.equals(host, auditLog.host) &&
               Objects.equals(queryId, auditLog.queryId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(username, query, effect, action, environment, startTime, host, queryId);
    }
    
    @Override
    public String toString() {
        return "AuditLog{" +
               "username='" + username + '\'' +
               ", query='" + query + '\'' +
               ", effect='" + effect + '\'' +
               ", action='" + action + '\'' +
               ", environment='" + environment + '\'' +
               ", startTime=" + startTime +
               ", host='" + host + '\'' +
               ", queryId='" + queryId + '\'' +
               '}';
    }
}