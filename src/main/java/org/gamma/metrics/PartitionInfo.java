package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record PartitionInfo(String sourceId, String partitionId, Status status, Duration duration, String threadName,
                            List<BatchInfo> batchMetrics, Throwable failureCause) implements HasStatus {
    public PartitionInfo(String sourceId, String partitionId, Status status, Duration duration, String threadName, List<BatchInfo> batchMetrics) {
        this(sourceId, partitionId, status, duration, threadName, batchMetrics, null);
    }
}

