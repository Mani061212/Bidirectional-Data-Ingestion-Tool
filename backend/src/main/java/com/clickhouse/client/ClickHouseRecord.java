package com.clickhouse.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.clickhouse.data.ClickHouseValue;

public class ClickHouseRecord {
    private final List<Object> values;
    private final List<String> columnNames;
    private final Map<String, Object> recordMap;

    public ClickHouseRecord(List<Object> values, List<String> columnNames) {
        this.values = values != null ? values : new ArrayList<>();
        this.columnNames = columnNames != null ? columnNames : new ArrayList<>();
        this.recordMap = null;
    }

    public ClickHouseRecord(Map<String, Object> recordMap) {
        this.recordMap = recordMap;
        this.values = new ArrayList<>(recordMap.values());
        this.columnNames = new ArrayList<>(recordMap.keySet());
    }

    public Object getValue(int index) {
        if (index < 0 || index >= values.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + values.size());
        }
        return values.get(index);
    }

    public Object getValue(String columnName) {
        int index = columnNames.indexOf(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        return getValue(index);
    }

    public int size() {
        return values.size();
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

	public com.clickhouse.client.ClickHouseValue get(int i) {
		// TODO Auto-generated method stub
		return null;
	}
}
