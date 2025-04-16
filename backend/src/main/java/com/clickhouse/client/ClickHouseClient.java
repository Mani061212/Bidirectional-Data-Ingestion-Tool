package com.clickhouse.client;

public interface ClickHouseClient extends AutoCloseable {
    QueryResponse query(String sql);
    
    static Builder builder() {
        return new ClickHouseClientImpl.Builder();
    }
    
    interface QueryResponse {
        ClickHouseResponse executeAndWait() throws Exception;
    }
    
    interface Builder {
        Builder node(ClickHouseNode node);
        Builder database(String database);
        Builder username(String username);
        Builder password(String password);
        ClickHouseClient build();
    }
} 