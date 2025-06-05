package org.gamma.processing;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

// Helper record for intermediate processing result (can be private inner record)
public record ProcessingResult(int batchId, String batchName, Instant batchStart, String threadName,
                                List<Path> batchData, Map<String, String> filesParsed) {
}
