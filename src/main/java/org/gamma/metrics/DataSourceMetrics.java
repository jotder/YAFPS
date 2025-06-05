package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record DataSourceMetrics(String sourceId, String sourceName, Status status, Duration duration, String threadName,
                             List<PartitionMetrics> partitionMetrics, Throwable failureCause) implements HasStatus {
        public DataSourceMetrics(String sourceId, String sourceName, Status status, Duration duration, String threadName, List<PartitionMetrics> partitionMetrics) {
            this(sourceId, sourceName, status, duration, threadName, partitionMetrics, null);
        }
    }


