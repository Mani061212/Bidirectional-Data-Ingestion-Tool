package com.clickhouse.client;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class ClickHouseResponse implements Iterable<ClickHouseRecord> {
    private final List<ClickHouseRecord> records;
    private final List<String> columnNames;

    public ClickHouseResponse(List<ClickHouseRecord> records, List<String> columnNames) {
        this.records = records;
        this.columnNames = columnNames;
    }

    public List<ClickHouseRecord> records() {
        return new ArrayList<>(records);
    }

    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    public int size() {
        return records.size();
    }

    @Override
    public Iterator<ClickHouseRecord> iterator() {
        return records.iterator();
    }

    public ClickHouseResponse executeAndWait() {
        return this; // Since we're already executing synchronously in ClickHouseClient
    }
} 