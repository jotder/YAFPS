package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record DataSourceInfo(String sourceId, String sourceName, Status status, Duration duration, String threadName,
                             List<PartitionInfo> partitionInfo, Throwable failureCause) implements HasStatus {
    public DataSourceInfo(String sourceId, String sourceName, Status status, Duration duration, String threadName, List<PartitionInfo> partitionMetrics) {
        this(sourceId, sourceName, status, duration, threadName, partitionMetrics, null);
    }
}


