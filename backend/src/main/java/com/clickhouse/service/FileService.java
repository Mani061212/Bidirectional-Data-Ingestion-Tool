package com.clickhouse.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

@Service
public class FileService {
    private static final Logger logger = Logger.getLogger(FileService.class.getName());
    private static final String UPLOAD_DIR = "uploads";

    public FileService() {
        createUploadDirectory();
    }

    private void createUploadDirectory() {
        try {
            Files.createDirectories(Paths.get(UPLOAD_DIR));
        } catch (IOException e) {
            logger.severe("Could not create upload directory: " + e.getMessage());
        }
    }

    public String saveUploadedFile(MultipartFile file) throws Exception {
        if (file.isEmpty()) {
            throw new Exception("Failed to store empty file");
        }

        try {
            String originalFilename = file.getOriginalFilename();
            String fileExtension = originalFilename.substring(originalFilename.lastIndexOf("."));
            String newFilename = UUID.randomUUID().toString() + fileExtension;
            Path destinationFile = Paths.get(UPLOAD_DIR).resolve(newFilename).normalize().toAbsolutePath();

            // Check that the destination is within the upload directory
            if (!destinationFile.getParent().equals(Paths.get(UPLOAD_DIR).toAbsolutePath())) {
                throw new Exception("Cannot store file outside upload directory");
            }

            // Copy the file to the destination
            Files.copy(file.getInputStream(), destinationFile);

            return destinationFile.toString();
        } catch (IOException e) {
            logger.severe("Error saving uploaded file: " + e.getMessage());
            throw new Exception("Failed to store file: " + e.getMessage());
        }
    }

    public List<String> getFileHeaders(String filePath, String delimiter) throws Exception {
        try (FileReader reader = new FileReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                 .setDelimiter(delimiter.charAt(0))
                 .setHeader()
                 .build()
                 .parse(reader)) {
            
            return new ArrayList<>(parser.getHeaderNames());
        } catch (IOException e) {
            logger.severe("Error reading file headers: " + e.getMessage());
            throw new Exception("Failed to read file headers: " + e.getMessage());
        }
    }

    public void deleteFile(String filePath) {
        try {
            Files.deleteIfExists(Paths.get(filePath));
        } catch (IOException e) {
            logger.warning("Error deleting file: " + e.getMessage());
        }
    }

    public void validateFileFormat(String filePath, String delimiter) throws IOException {
        try (FileReader fileReader = new FileReader(filePath);
             CSVParser csvParser = CSVFormat.DEFAULT.builder()
                 .setDelimiter(delimiter.charAt(0))
                 .build()
                 .parse(fileReader)) {
            // Just try to parse the file to validate its format
            csvParser.getRecords();
        }
    }
} 