package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record BatchInfo(String microBatchId, String batchName, Status status, Duration duration, String threadName,
                        Throwable failureCause, List<LoadingInfo> loadingInfo) implements HasStatus {
}