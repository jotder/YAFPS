package org.gamma.metrics;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.processing.MergedFileWriter;

import java.io.File;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Helper methods for creating metrics instances, especially for failure cases,
 * and determining overall status.
 */
public final class StatusHelper {

    private StatusHelper() {
    } // Prevent instantiation

    // --- Failure Metric Creators ---

    //    public static DataSourceInfo createFailedDataSourceMetrics(YamlSourceConfigAdapter conf, Throwable cause) {
//        return new DataSourceInfo(conf.sourceID(), conf.sourceName(), Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), List.of(), cause);
//    }
    public static DataSourceInfo createFailedDataSourceInfo(final EtlPipelineItem conf, final Throwable cause) {
        SourceItem pollInf = conf.sources().getFirst();
        return new DataSourceInfo(pollInf.sourceId(), conf.pipelineName(), Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), List.of(), cause);
    }

    public static PartitionInfo createFailedPartitionInfo(String sourceId, String partitionId, Throwable cause) {
        return new PartitionInfo(sourceId, partitionId, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), List.of(), cause);
    }

    public static BatchInfo createFailedBatchInfo(String batchId, String batchName, Throwable cause) {
        return new BatchInfo(batchId, batchName, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), cause, List.of());
    }

    public static LoadingInfo createFailedLoadInfo(String fileName, String targetTable, Throwable cause) {
        return new LoadingInfo(fileName, targetTable, Status.FAIL, Duration.ZERO, Thread.currentThread().getName(), cause);
    }

    // --- Status Determination ---

    /**
     * Determines the overall status based on a collection of results,
     * considering if any sub-tasks failed or if expected tasks produced no results.
     */
    public static <T extends HasStatus> Status determineOverallStatus(
            final List<T> results,
            final int expectedTaskCount,
            final String levelName,
            final Object identifier) {

        final String idStr = identifier != null ? identifier.toString() : "N/A";

        // Check if any collected result explicitly has a FAIL status
        final boolean anySubTaskFailed = results.stream().anyMatch(r -> r.status() == Status.FAIL);
        if (anySubTaskFailed) {
            System.err.printf("  %s %s: Marked as FAIL because at least one sub-task reported FAIL status.%n", levelName, idStr);
            return Status.FAIL;
        }

        // Check if the number of successful results matches the number of submitted tasks
        // Note: This assumes that a failure during future execution prevents it from being added to 'results'.
        if (results.size() < expectedTaskCount) {
            System.err.printf("  %s %s: Marked as FAIL because some sub-tasks failed to produce results (%d/%d succeeded).%n",
                    levelName, idStr, results.size(), expectedTaskCount);
            return Status.FAIL;
        }

        // If no sub-tasks failed and all expected tasks produced results, it's a PASS
        if (expectedTaskCount == 0) {
            System.out.printf("  %s %s: Marked as PASS (no sub-tasks were expected or executed).%n", levelName, idStr);
        } else {
            System.out.printf("  %s %s: Marked as PASS (%d/%d sub-tasks succeeded).%n", levelName, idStr, results.size(), expectedTaskCount);
        }
        return Status.PASS;
    }

    public static RouteInfo createRouteRecord(String dataSource, Path source, Path merge, long count) {
        return new RouteInfo(dataSource, source, merge, count);
    }


    public static FileInfo createMergedRecord(String dataSource, MergedFileWriter w) {
        File file = w.mFlePath.toFile();
        String fileID = "", fileName = file.getName();
        long fileSize = file.length();
        Timestamp lastModTs = Timestamp.from(Instant.ofEpochSecond(file.lastModified()));
        String sourceFileUrl = w.mFlePath.toString();
        long recCount = w.getRecordCount(), failCount = 0;
        String fileType = "MERGED", task = "OUTPUT";
        Timestamp startTs = null;
        long duration = 0;
        String recordStart = "", recordEnd = "";
        return new FileInfo(fileID, dataSource, fileName, fileSize, lastModTs, recCount, 0, fileType,
                task, startTs, duration, Status.PASS, recordStart, recordEnd, "");

//        this.path = Util.getFileInf(this.dataSource, this.filePath);
//        return new ParseInfo(this.path, 0L, 0L, "MERGED", "OUTPUT", Timestamp.from(this.startTime), 0, "", "", "");
//        String fileID, String dataSource, String fileName, long fileSize, Timestamp lastModTs,
//        String sourceFileUrl, long recCount, long failCount, String fileType, String task,
//        Timestamp startTs, long duration, String recordStart, String recordEnd, String message, HasStatus

    }

    public static FileInfo createFileInfo(Path path, String dataSource, long parseCount, long failCount, Timestamp startTs, long duration,
                                          Status status, String startRecord, String endRecord, String fileErrorMsg) {
        File file = path.toFile();
        String fileID = "", fileName = file.getName();
        long fileSize = file.length();

        Timestamp lastModTs = Timestamp.from(Instant.ofEpochSecond(file.lastModified()));
        return new FileInfo(fileID, dataSource, fileName, fileSize, lastModTs, parseCount, failCount, "SOURCE", "PROCESS",
                Timestamp.from(Instant.now()), duration, status,
                startRecord, endRecord, fileErrorMsg == null ? "" : fileErrorMsg);
    }

    public static FileInfo createMatricesInfo() {
//        'new FileInfo(mfi, (long) matrixBuilder.summaryMatrix.size(), 0L, "MATRIX", "LOAD", Timestamp.from(Instant.now()), (System.nanoTime() - batchStart) / 1000000L, "PASS", recStart, recEnd, "");
        return null;
    }
}
    