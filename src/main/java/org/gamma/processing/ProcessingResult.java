package org.gamma.processing;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.gamma.metrics.FileInfo; // Assuming FileInfo is in org.gamma.metrics

public record ProcessingResult(
    int batchId,
    String batchName,
    Instant batchStart,
    String threadName,
    List<Path> batchData,
    Map<String, String> filesToLoad,
    List<FileInfo> fileInfoList
) {}
