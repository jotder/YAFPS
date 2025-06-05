package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record PartitionMetrics(String sourceId, String partitionId, Status status, Duration duration, String threadName,
                               List<BatchMetrics> batchMetrics, Throwable failureCause) implements HasStatus {
    public PartitionMetrics(String sourceId, String partitionId, Status status, Duration duration, String threadName, List<BatchMetrics> batchMetrics) {
        this(sourceId, partitionId, status, duration, threadName, batchMetrics, null);
    }
}

