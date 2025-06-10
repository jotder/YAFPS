package org.gamma.metrics;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public  record ProcessingResult(
        String batchId,
        String batchName,
        Instant batchStart,
        String threadName,
        List<Path> batchData, // Assuming Path is FQN or imported (it is)
        Map<String, String> filesToLoad,
        List<FileInfo> fileInfoList // New field (FileInfo is in same package)
    ) {
    }