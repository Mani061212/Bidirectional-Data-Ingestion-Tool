package com.clickhouse.service;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.model.ClickHouseConnection;
import com.clickhouse.model.ColumnSelection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class DataPreviewService {
    private static final Logger logger = Logger.getLogger(DataPreviewService.class.getName());

    public List<List<String>> previewClickHouseData(ClickHouseConnection config, String table, 
                                                  ColumnSelection columns) throws Exception {
        logger.info("Previewing ClickHouse data for table: " + table);
        logger.info("Columns: " + columns.getColumns());
        try (ClickHouseClient client = createClient(config)) {
            String query = String.format("SELECT %s FROM %s LIMIT 100", 
                String.join(", ", columns.getColumns()), 
                table);
            logger.info("Executing query: " + query);
            ClickHouseResponse response = client.query(query).executeAndWait();
            
            List<List<String>> previewData = new ArrayList<>();
            for (ClickHouseRecord record : response.records()) {
                List<String> row = new ArrayList<>();
                for (int i = 0; i < columns.getColumns().size(); i++) {
                    row.add(record.get(i).asString());
                }
                previewData.add(row);
            }
            logger.info("Retrieved " + previewData.size() + " records");
            return previewData;
        } catch (Exception e) {
            logger.severe("Error previewing ClickHouse data: " + e.getMessage());
            e.printStackTrace();
            throw new Exception("Failed to preview data: " + e.getMessage());
        }
    }

    public List<Map<String, String>> previewFileData(String filePath, String delimiter, List<String> columns) throws Exception {
        try (Reader reader = new FileReader(filePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader()
                .setTrim(true)  // Remove extra spaces
                .build()
                .parse(reader);
            
            List<Map<String, String>> previewData = new ArrayList<>();
            int count = 0;
            
            for (CSVRecord record : records) {
                if (count >= 100) break;
                
                Map<String, String> row = new HashMap<>();
                for (String column : columns) {
                    try {
                        row.put(column, record.get(column).trim());
                    } catch (IllegalArgumentException e) {
                        logger.severe("Column not found: " + column + ". Available columns: " + record.getHeaderNames());
                        throw new Exception("Column not found: " + column + ". Available columns: " + record.getHeaderNames());
                    }
                }
                previewData.add(row);
                count++;
            }
            
            logger.info("Successfully previewed " + count + " records from file");
            return previewData;
        } catch (Exception e) {
            logger.severe("Error previewing file data: " + e.getMessage());
            throw new Exception("Failed to preview file data: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> previewClickHouseData(String tableName, List<String> columns) {
        List<Map<String, Object>> preview = new ArrayList<>();
        try (ClickHouseClient client = createClient()) {
            String columnList = columns.isEmpty() ? "*" : String.join(", ", columns);
            String sql = String.format("SELECT %s FROM %s LIMIT 100", columnList, tableName);
            
            ClickHouseResponse response = client.query(sql).executeAndWait();
            for (ClickHouseRecord record : response.records()) {
                Map<String, Object> row = new HashMap<>();
                List<String> columnNames = record.getColumnNames();
                for (int i = 0; i < columnNames.size(); i++) {
                    row.put(columnNames.get(i), record.getValue(i));
                }
                preview.add(row);
            }
        } catch (Exception e) {
            logger.severe("Error previewing ClickHouse data: " + e.getMessage());
            throw new RuntimeException("Failed to preview data", e);
        }
        return preview;
    }

    private ClickHouseClient createClient(ClickHouseConnection config) {
        return ClickHouseClient.builder()
            .node(ClickHouseNode.of(config.getHost(), Map.of("port", String.valueOf(config.getPort()))))
            .database(config.getDatabase())
            .username(config.getUser())
            .password(config.getJwtToken())
            .build();
    }

    private ClickHouseClient createClient() {
        return ClickHouseClient.builder()
            .node(ClickHouseNode.of("localhost", Map.of("port", "8123")))
            .database("default")
            .username("default")
            .password("")
            .build();
    }
} 