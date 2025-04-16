package com.clickhouse.model;

import lombok.Data;

@Data
public class PreviewRequest {
    private ClickHouseConnection config;
    private ColumnSelection columns;

    public ClickHouseConnection getConfig() {
        return config;
    }

    public void setConfig(ClickHouseConnection config) {
        this.config = config;
    }

    public ColumnSelection getColumns() {
        return columns;
    }

    public void setColumns(ColumnSelection columns) {
        this.columns = columns;
    }
} 