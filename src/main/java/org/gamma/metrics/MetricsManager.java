package org.gamma.metrics;

import org.gamma.config.EtlPipelineItem; // Added for createFailedDataSourceMetrics
import org.gamma.config.SourceItem;    // Added for createFailedDataSourceMetrics

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MetricsManager {

    // --- Status Enum ---
    public static enum Status {
        PASS, FAIL, PARTIAL
    }

    // --- Helper Interface for Status ---
    public interface HasStatus {
        Status status();
    }

    // --- Metrics ---
    public static record LoadInfo(String fileName, String targetTable, Status status, Duration duration, String threadName,
                           Throwable failureCause) implements HasStatus {
    }

    public static record BatchInfo(int microBatchId, String batchName, Status status, Duration duration, String threadName,
                            Throwable failureCause, List<LoadInfo> loadInfo) implements HasStatus {
    }

    public static record PartitionInfo(String sourceId, String partitionId, Status status, Duration duration, String threadName,
                                List<BatchInfo> batchMetrics, Throwable failureCause) implements HasStatus {
        public PartitionInfo(String sourceId, String partitionId, Status status, Duration duration, String threadName, List<BatchInfo> batchMetrics) {
            this(sourceId, partitionId, status, duration, threadName, batchMetrics, null);
        }
    }

    public static record DataSourceInfo(String sourceId, String sourceName, Status status, Duration duration, String threadName,
                                 List<PartitionInfo> partitionInfo, Throwable failureCause) implements HasStatus {
        public DataSourceInfo(String sourceId, String sourceName, Status status, Duration duration, String threadName, List<PartitionInfo> partitionMetrics) {
            this(sourceId, sourceName, status, duration, threadName, partitionMetrics, null);
        }
    }

    public static record ExecutionInfo(Duration totalDuration, List<DataSourceInfo> dataSourceInfo) {
    }

    public static record ProcessingResult(int batchId, String batchName, Instant batchStart, String threadName,
                                          List<Path> batchData, Map<String, String> filesToLoad) {
    }

    public static record LoadTaskContext(String fileName, String tableName, CompletableFuture<LoadInfo> future) {
    }

    // --- Failure Metric Helpers ---
    public static DataSourceInfo createFailedDataSourceMetrics(final EtlPipelineItem conf, final Throwable cause) {
        SourceItem pollInf = conf.sources().getFirst();
        return new DataSourceInfo(pollInf.sourceId(), conf.pipelineName(), Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), List.of(), cause);
    }

    public static PartitionInfo createFailedPartitionMetrics(final String sourceId, final String partitionId, final Throwable cause) {
        return new PartitionInfo(sourceId, partitionId, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), List.of(), cause);
    }

    public static BatchInfo createFailedBatchMetrics(final int batchId, final String batchName, final Throwable cause) {
        return new BatchInfo(batchId, batchName, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), cause, List.of());
    }

    public static LoadInfo createFailedLoadMetrics(final String fileName, final String targetTable, final Throwable cause) {
        return new LoadInfo(fileName, targetTable, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), cause);
    }

    public static <T extends HasStatus> Status determineOverallStatus(
            final List<T> results, final int expectedTaskCount, final String levelName, final Object identifier) {
        final String idStr = identifier != null ? identifier.toString() : "N/A";
        if (results.stream().anyMatch(r -> r.status() == Status.FAIL)) { // Uses MetricsManager.Status
            System.err.printf("  %s %s: FAIL (sub-task failed).%n", levelName, idStr);
            return Status.FAIL; // Uses MetricsManager.Status
        }
        if (results.size() < expectedTaskCount) {
            System.err.printf("  %s %s: FAIL (missing results %d/%d).%n", levelName, idStr, results.size(), expectedTaskCount);
            return Status.FAIL; // Uses MetricsManager.Status
        }
        System.out.printf("  %s %s: PASS (%d/%d sub-tasks succeeded).%n", levelName, idStr, results.size(), expectedTaskCount);
        return Status.PASS; // Uses MetricsManager.Status
    }
}
