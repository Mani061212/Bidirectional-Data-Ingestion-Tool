package com.clickhouse.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickHouseClientImpl implements ClickHouseClient {
    private final Connection connection;
    private static final String JDBC_URL_FORMAT = "jdbc:clickhouse://%s:%d/%s";

    private ClickHouseClientImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public QueryResponse query(String sql) {
        return new QueryResponseImpl(sql, this);
    }

    ClickHouseResponse executeQuery(String sql) throws Exception {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            int columnCount = rs.getMetaData().getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(rs.getMetaData().getColumnName(i));
            }

            List<ClickHouseRecord> records = new ArrayList<>();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    values.add(value != null ? value.toString() : null);
                }
                Map<String, Object> recordMap = new HashMap<>();
                for (int i = 0; i < columnCount; i++) {
                    recordMap.put(columnNames.get(i), values.get(i));
                }
                records.add(new ClickHouseRecord(recordMap));
            }

            return new ClickHouseResponse(records, columnNames);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private static class QueryResponseImpl implements QueryResponse {
        private final String sql;
        private final ClickHouseClientImpl client;

        QueryResponseImpl(String sql, ClickHouseClientImpl client) {
            this.sql = sql;
            this.client = client;
        }

        @Override
        public ClickHouseResponse executeAndWait() throws Exception {
            return client.executeQuery(sql);
        }
    }

    public static class Builder implements ClickHouseClient.Builder {
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;

        @Override
        public Builder node(ClickHouseNode node) {
            this.host = node.getHost();
            this.port = node.getPort();
            return this;
        }

        @Override
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        @Override
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        @Override
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        @Override
        public ClickHouseClient build() {
            try {
                String url = String.format(JDBC_URL_FORMAT, host, port, database);
                Connection conn = DriverManager.getConnection(url, username, password);
                return new ClickHouseClientImpl(conn);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create ClickHouseClient: " + e.getMessage(), e);
            }
        }
    }
} 