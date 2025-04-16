package com.clickhouse.service;

import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProgressService {
    private final Map<String, Integer> progressMap = new ConcurrentHashMap<>();

    public void updateProgress(String taskId, int progress) {
        progressMap.put(taskId, progress);
    }

    public int getProgress(String taskId) {
        return progressMap.getOrDefault(taskId, 0);
    }

    public void removeProgress(String taskId) {
        progressMap.remove(taskId);
    }
} 