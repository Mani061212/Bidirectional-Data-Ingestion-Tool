package com.clickhouse.client;

import java.util.Map;

public class ClickHouseNode {
    private final String host;
    private final int port;

    private ClickHouseNode(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static ClickHouseNode of(String host, Map<String, String> properties) {
        int port = Integer.parseInt(properties.getOrDefault("port", "8123"));
        return new ClickHouseNode(host, port);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
} 