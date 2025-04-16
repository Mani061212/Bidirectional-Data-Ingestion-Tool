package com.clickhouse.service;

import com.clickhouse.model.ClickHouseConnection;
import com.clickhouse.model.ColumnSelection;
import com.clickhouse.model.FlatFileConfig;
import com.clickhouse.model.JoinConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class ClickHouseService {
    private static final Logger logger = Logger.getLogger(ClickHouseService.class.getName());

    public List<String> getTables(ClickHouseConnection config) {
        logger.info("Fetching tables for database: " + config.getDatabase());
        List<String> tables = new ArrayList<>();
        try {
            String query = URLEncoder.encode("SHOW TABLES FROM " + config.getDatabase(), StandardCharsets.UTF_8);
            String url = String.format("http://%s:%d",
                config.getHost(),
                config.getPort());
            
            logger.info("Connecting to ClickHouse at: " + url);
            
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-ClickHouse-User", config.getUser())
                .header("X-ClickHouse-Key", config.getJwtToken() != null ? config.getJwtToken() : "")
                .header("X-ClickHouse-Database", config.getDatabase())
                .header("X-ClickHouse-Format", "TabSeparated")
                .POST(HttpRequest.BodyPublishers.ofString("SHOW TABLES"))
                .build();
            
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            logger.info("Response status code: " + response.statusCode());
            logger.info("Response body: " + response.body());
            
            if (response.statusCode() != 200) {
                throw new RuntimeException("ClickHouse query failed with status code: " + response.statusCode() + ", body: " + response.body());
            }
            
            String[] lines = response.body().split("\n");
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    logger.info("Found table: " + line.trim());
                    tables.add(line.trim());
                }
            }
            
            logger.info("Found " + tables.size() + " tables");
            return tables;
        } catch (Exception e) {
            logger.severe("Error fetching tables: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to fetch tables: " + e.getMessage(), e);
        }
    }

    public List<Map<String, String>> getColumns(ClickHouseConnection config, String tableName) {
        System.out.println("Fetching columns for table: " + tableName);
        List<Map<String, String>> columns = new ArrayList<>();
        try {
            String query = URLEncoder.encode("DESCRIBE " + tableName, StandardCharsets.UTF_8);
            String url = String.format("http://%s:%d/?query=%s",
                config.getHost(),
                config.getPort(),
                query);
            
            System.out.println("Executing query: " + url);
            
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-ClickHouse-User", config.getUser())
                .header("X-ClickHouse-Key", config.getJwtToken() != null ? config.getJwtToken() : "")
                .build();
            
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("ClickHouse query failed with status code: " + response.statusCode() + ", body: " + response.body());
            }
            
            String[] lines = response.body().split("\n");
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        Map<String, String> columnInfo = new HashMap<>();
                        columnInfo.put("name", parts[0].trim());
                        columnInfo.put("type", parts[1].trim());
                        columns.add(columnInfo);
                    }
                }
            }
            return columns;
        } catch (Exception e) {
            System.err.println("Error fetching columns: " + e.getMessage());
            throw new RuntimeException("Failed to fetch columns: " + e.getMessage());
        }
    }

    private HttpResponse<String> executeQuery(ClickHouseConnection config, String query) throws Exception {
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
        String url = String.format("http://%s:%d/?query=%s",
            config.getHost(),
            config.getPort(),
            encodedQuery);
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("X-ClickHouse-User", config.getUser())
            .header("X-ClickHouse-Key", config.getJwtToken() != null ? config.getJwtToken() : "")
            .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("ClickHouse query failed with status code: " + response.statusCode() + ", body: " + response.body());
        }
        return response;
    }

    public String exportToFile(ClickHouseConnection config, String table, 
                             ColumnSelection columns, FlatFileConfig fileConfig) throws Exception {
        try (FileWriter writer = new FileWriter(fileConfig.getFilePath());
             CSVPrinter csvPrinter = new CSVPrinter(writer, 
                 CSVFormat.DEFAULT.builder()
                     .setDelimiter(fileConfig.getDelimiter().charAt(0))
                     .build())) {
            
            // Write headers
            csvPrinter.printRecord(columns.getColumns());
            
            // Write data
            String query = String.format("SELECT %s FROM %s FORMAT TabSeparated", 
                String.join(", ", columns.getColumns()), 
                table);
                
            HttpResponse<String> response = executeQuery(config, query);
            String[] lines = response.body().split("\n");
            
            int recordCount = 0;
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    String[] values = line.split("\t");
                    csvPrinter.printRecord((Object[]) values);
                    recordCount++;
                }
            }
            
            return String.format("Successfully exported %d records to %s", 
                recordCount, fileConfig.getFilePath());
        } catch (Exception e) {
            System.err.println("Error exporting to file: " + e.getMessage());
            throw new Exception("Failed to export data: " + e.getMessage());
        }
    }

    public String importFromFile(ClickHouseConnection config, String table,
                               ColumnSelection columns, FlatFileConfig fileConfig) throws Exception {
        try (Reader reader = new FileReader(fileConfig.getFilePath())) {
            // Create table if not exists
            String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
                table,
                String.join(", ", columns.getColumns().stream()
                    .map(col -> col + " String")
                    .toList())
            );
            
            HttpResponse<String> response = executeQuery(config, createTableQuery);
            
            // Read CSV file
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                .setDelimiter(fileConfig.getDelimiter().charAt(0))
                .setHeader()
                .build()
                .parse(reader);
            
            int batchSize = 1000;
            List<String> values = new ArrayList<>();
            int recordCount = 0;
            
            for (CSVRecord record : records) {
                List<String> rowValues = new ArrayList<>();
                for (String column : columns.getColumns()) {
                    String value = String.valueOf(record.get(column));
                    rowValues.add("'" + value.replace("'", "''") + "'");
                }
                values.add("(" + String.join(", ", rowValues) + ")");
                recordCount++;
                
                // Batch insert every 1000 records
                if (values.size() >= batchSize) {
                    executeBatchInsert(config, table, columns.getColumns(), values);
                    values.clear();
                }
            }
            
            // Insert remaining records
            if (!values.isEmpty()) {
                executeBatchInsert(config, table, columns.getColumns(), values);
            }
            
            return String.format("Successfully imported %d records from %s", 
                recordCount, fileConfig.getFilePath());
        } catch (Exception e) {
            logger.severe("Error importing from file: " + e.getMessage());
            throw new Exception("Failed to import data: " + e.getMessage());
        }
    }

    private void executeBatchInsert(ClickHouseConnection config, String table, 
                                  List<String> columns, List<String> values) throws Exception {
        String insertQuery = String.format("INSERT INTO %s (%s) VALUES %s",
            table,
            String.join(", ", columns),
            String.join(", ", values));
            
        HttpResponse<String> response = executeQuery(config, insertQuery);
    }

    public String joinTables(ClickHouseConnection config, JoinConfig joinConfig,
                           ColumnSelection columns, FlatFileConfig fileConfig) throws Exception {
        try (FileWriter writer = new FileWriter(fileConfig.getFilePath());
             CSVPrinter csvPrinter = new CSVPrinter(writer, 
                 CSVFormat.DEFAULT.builder()
                     .setDelimiter(fileConfig.getDelimiter().charAt(0))
                     .build())) {
            
            // Write headers
            csvPrinter.printRecord(columns.getColumns());
            
            // Write data
            String query = buildJoinQuery(joinConfig, columns) + " FORMAT TabSeparated";
            HttpResponse<String> response = executeQuery(config, query);
            String[] lines = response.body().split("\n");
            
            int recordCount = 0;
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    String[] values = line.split("\t");
                    csvPrinter.printRecord((Object[]) values);
                    recordCount++;
                }
            }
            
            return String.format("Successfully exported %d records from joined tables to %s", 
                recordCount, fileConfig.getFilePath());
        } catch (Exception e) {
            logger.severe("Error joining tables: " + e.getMessage());
            throw new Exception("Failed to join tables: " + e.getMessage());
        }
    }

    private String buildJoinQuery(JoinConfig joinConfig, ColumnSelection columns) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(String.join(", ", columns.getColumns()))
             .append(" FROM ").append(joinConfig.getSourceTable());
        
        if (joinConfig.getJoinTable() != null) {
            query.append(" JOIN ").append(joinConfig.getJoinTable())
                 .append(" ON ").append(joinConfig.getJoinCondition());
        }
        
        return query.toString();
    }
} 