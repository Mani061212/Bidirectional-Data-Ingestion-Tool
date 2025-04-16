package com.clickhouse.client;

public class ClickHouseValue {
    private final Object value;

    public ClickHouseValue(Object value) {
        this.value = value;
    }

    public String asString() {
        return value != null ? String.valueOf(value) : null;
    }

    public Integer asInteger() {
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Cannot convert value to Integer: " + value);
        }
    }

    public Long asLong() {
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Cannot convert value to Long: " + value);
        }
    }

    public Double asDouble() {
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Cannot convert value to Double: " + value);
        }
    }

    public Boolean asBoolean() {
        if (value == null) return null;
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String strValue = value.toString().trim().toLowerCase();
        if (strValue.equals("1") || strValue.equals("true") || strValue.equals("yes")) {
            return true;
        }
        if (strValue.equals("0") || strValue.equals("false") || strValue.equals("no")) {
            return false;
        }
        throw new IllegalStateException("Cannot convert value to Boolean: " + value);
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return asString();
    }
} 