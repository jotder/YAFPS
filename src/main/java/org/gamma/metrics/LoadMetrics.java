package org.gamma.metrics;

import java.time.Duration;

public record LoadMetrics(String fileName, String targetTable, Status status, Duration duration, String threadName,
                          Throwable failureCause) implements HasStatus {
}