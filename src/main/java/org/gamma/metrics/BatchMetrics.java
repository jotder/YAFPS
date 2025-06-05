package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record BatchMetrics(int microBatchId, String batchName, Status status, Duration duration, String threadName,
                           Throwable failureCause, List<LoadMetrics> loadMetrics) implements HasStatus {
}