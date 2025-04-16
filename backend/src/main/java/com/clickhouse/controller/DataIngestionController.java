package com.clickhouse.controller;

import com.clickhouse.model.ClickHouseConnection;
import com.clickhouse.model.ColumnSelection;
import com.clickhouse.model.FlatFileConfig;
import com.clickhouse.model.JoinConfig;
import com.clickhouse.model.PreviewRequest;
import com.clickhouse.service.ClickHouseService;
import com.clickhouse.service.DataPreviewService;
import com.clickhouse.service.FileService;
import com.clickhouse.service.ProgressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000")
public class DataIngestionController {

    private static final Logger log = LoggerFactory.getLogger(DataIngestionController.class);

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FileService fileService;

    @Autowired
    private DataPreviewService dataPreviewService;

    @Autowired
    private ProgressService progressService;

    @GetMapping("/tables")
    public ResponseEntity<List<String>> getTables(
            @RequestParam String host,
            @RequestParam int port,
            @RequestParam String database,
            @RequestParam String user,
            @RequestParam(required = false) String jwtToken) {
        log.info("Received request for tables with params: host=" + host + 
            ", port=" + port + ", database=" + database + ", user=" + user);
        try {
            ClickHouseConnection config = new ClickHouseConnection();
            config.setHost(host);
            config.setPort(port);
            config.setDatabase(database);
            config.setUser(user);
            config.setJwtToken(jwtToken != null ? jwtToken : "");
            
            List<String> tables = clickHouseService.getTables(config);
            log.info("Successfully retrieved " + tables.size() + " tables");
            return ResponseEntity.ok(tables);
        } catch (Exception e) {
            log.error("Error processing request: " + e.getMessage());
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/columns")
    public ResponseEntity<List<Map<String, String>>> getColumns(
            @RequestParam String host,
            @RequestParam int port,
            @RequestParam String database,
            @RequestParam String user,
            @RequestParam String table,
            @RequestParam(required = false) String jwtToken) {
        try {
            ClickHouseConnection config = new ClickHouseConnection();
            config.setHost(host);
            config.setPort(port);
            config.setDatabase(database);
            config.setUser(user);
            config.setJwtToken(jwtToken != null ? jwtToken : "");
            return ResponseEntity.ok(clickHouseService.getColumns(config, table));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter) {
        try {
            String filePath = fileService.saveUploadedFile(file);
            fileService.validateFileFormat(filePath, delimiter);
            return ResponseEntity.ok(filePath);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error uploading file: " + e.getMessage());
        }
    }

    @GetMapping("/file-headers")
    public ResponseEntity<List<String>> getFileHeaders(
            @RequestParam String filePath,
            @RequestParam String delimiter) {
        try {
            return ResponseEntity.ok(fileService.getFileHeaders(filePath, delimiter));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/preview/clickhouse")
    public ResponseEntity<List<List<String>>> previewClickHouseData(
            @RequestParam String table,
            @RequestBody PreviewRequest request) {
        try {
            return ResponseEntity.ok(dataPreviewService.previewClickHouseData(request.getConfig(), table, request.getColumns()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/preview/file")
    public ResponseEntity<List<Map<String, String>>> previewFileData(
            @RequestParam String filePath,
            @RequestParam String delimiter,
            @RequestParam List<String> columns) {
        try {
            return ResponseEntity.ok(dataPreviewService.previewFileData(filePath, delimiter, columns));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/progress/{taskId}")
    public ResponseEntity<Integer> getProgress(@PathVariable String taskId) {
        return ResponseEntity.ok(progressService.getProgress(taskId));
    }

    @PostMapping("/ingest/clickhouse-to-file")
    public ResponseEntity<String> clickhouseToFile(
            @RequestBody ClickHouseConnection config,
            @RequestParam String table,
            @RequestBody ColumnSelection columns,
            @RequestBody FlatFileConfig fileConfig) {
        String taskId = UUID.randomUUID().toString();
        try {
            // Start a new thread for the ingestion process
            new Thread(() -> {
                try {
                    progressService.updateProgress(taskId, 10);
                    String result = clickHouseService.exportToFile(config, table, columns, fileConfig);
                    progressService.updateProgress(taskId, 100);
                    progressService.removeProgress(taskId);
                } catch (Exception e) {
                    progressService.updateProgress(taskId, -1);
                }
            }).start();
            
            return ResponseEntity.ok(taskId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @PostMapping("/ingest/file-to-clickhouse")
    public ResponseEntity<String> fileToClickhouse(
            @RequestBody ClickHouseConnection config,
            @RequestParam String table,
            @RequestBody ColumnSelection columns,
            @RequestBody FlatFileConfig fileConfig) {
        String taskId = UUID.randomUUID().toString();
        try {
            // Start a new thread for the ingestion process
            new Thread(() -> {
                try {
                    progressService.updateProgress(taskId, 10);
                    String result = clickHouseService.importFromFile(config, table, columns, fileConfig);
                    fileService.deleteFile(fileConfig.getFilePath());
                    progressService.updateProgress(taskId, 100);
                    progressService.removeProgress(taskId);
                } catch (Exception e) {
                    progressService.updateProgress(taskId, -1);
                }
            }).start();
            
            return ResponseEntity.ok(taskId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @PostMapping("/ingest/join-tables")
    public ResponseEntity<String> joinTables(
            @RequestBody ClickHouseConnection config,
            @RequestBody JoinConfig joinConfig,
            @RequestBody ColumnSelection columns,
            @RequestBody FlatFileConfig fileConfig) {
        String taskId = UUID.randomUUID().toString();
        try {
            // Start a new thread for the ingestion process
            new Thread(() -> {
                try {
                    progressService.updateProgress(taskId, 10);
                    String result = clickHouseService.joinTables(config, joinConfig, columns, fileConfig);
                    progressService.updateProgress(taskId, 100);
                    progressService.removeProgress(taskId);
                } catch (Exception e) {
                    progressService.updateProgress(taskId, -1);
                }
            }).start();
            
            return ResponseEntity.ok(taskId);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
} 