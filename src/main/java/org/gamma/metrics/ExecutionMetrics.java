package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record ExecutionMetrics(Duration totalDuration, List<DataSourceMetrics> dataSourceMetrics) {
}