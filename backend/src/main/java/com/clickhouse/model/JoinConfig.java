package com.clickhouse.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinConfig {
    private String sourceTable;
    private String joinTable;
    private String joinCondition;
    
    public String getSourceTable() {
        return sourceTable;
    }
    
    public String getJoinTable() {
        return joinTable;
    }
    
    public String getJoinCondition() {
        return joinCondition;
    }
    
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }
    
    public void setJoinTable(String joinTable) {
        this.joinTable = joinTable;
    }
    
    public void setJoinCondition(String joinCondition) {
        this.joinCondition = joinCondition;
    }
} 