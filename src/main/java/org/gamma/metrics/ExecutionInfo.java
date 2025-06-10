package org.gamma.metrics;

import java.time.Duration;
import java.util.List;

public record ExecutionInfo(Duration totalDuration, List<DataSourceInfo> dataSourceInfo) {
}