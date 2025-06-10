package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.DataSourceInfo;
import org.gamma.metrics.PartitionInfo;
import org.gamma.metrics.Status;
import org.gamma.metrics.StatusHelper;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.Utils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processes a single data source, handling locking, partition discovery, and execution.
 */
public class DataSourceHandler {

    private final EtlPipelineItem config;

    public DataSourceHandler(EtlPipelineItem config) {
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Processes the data source. Intended to be called asynchronously.
     * Handles locking and manages partition processing.
     */
    public DataSourceInfo process() throws IOException {
        final Instant sourceStart = Instant.now();
        final String sourceName = "%s".formatted(config.pipelineName());
        final String currentThreadName = Thread.currentThread().getName();
        final Path sourceDirPath = config.statusDir();
        System.out.printf("%nProcessing Data Source : %s on Thread %s%n", sourceName, currentThreadName);

        SourceItem cnf = config.sources().getFirst();

        List<Path> partitions;
        try {
            if (cnf.useSubDirAsPartition()) {
                partitions = Utils.getDirectoriesAsPartition(cnf.sourceDir(), cnf.dirFilter());
                System.out.printf("  Source %s: Found %d partition directories matching '%s'.%n",
                        sourceName, partitions.size(), cnf.dirFilter());
            } else {
                partitions = List.of(sourceDirPath);
                System.out.printf("  Source %s: Processing source directory '%s' as a single partition.%n", sourceName, cnf.sourceDir());
            }
        } catch (IOException e) {
            System.err.printf("!!! FATAL: Failed to list partitions for Source %s: %s%n", sourceName, e.getMessage());
            return StatusHelper.createFailedDataSourceInfo(config, e);
        }

        if (partitions.isEmpty() && cnf.useSubDirAsPartition()) {
            System.out.printf("  Source %s: No matching partition directories found. Skipping.%n", sourceName);
            return new DataSourceInfo(cnf.sourceId(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of());
        }
        System.out.printf("  Source %s: Submitting %d partitions (batchSize=%d).%n", sourceName, partitions.size(), cnf.batchSize());

        List<PartitionInfo> partitionResults;
        Status sourceStatus;
        int expectedPartitionCount = partitions.size();

        final ExecutorService service = Executors.newFixedThreadPool(cnf.numThreads(),
                ConcurrencyUtils.createPlatformThreadFactory(sourceName + "-partition-"));

        List<CompletableFuture<PartitionInfo>> partitionFutures = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(1);
        try {
            for (Path path : partitions) {
                String partitionId = path.getFileName() + "_" + counter.getAndIncrement();

                PartitionHandler handler = new PartitionHandler(config, partitionId, path);
                CompletableFuture<PartitionInfo> partitionFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return handler.handle(partitions.size()); // Call the process method
                            } catch (RuntimeException e) {  // Catch unexpected runtime exceptions from process()
                                System.err.printf("! Exception processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                e.printStackTrace(System.err);
                                // Create failed metrics if the processor itself threw unexpectedly
                                return StatusHelper.createFailedPartitionInfo(cnf.sourceId(), partitionId, e);
                            }
                        },
                        service
                );
                partitionFutures.add(partitionFuture);
            }

            partitionResults = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Partition",
                    partitionFutures, cnf.sourceId()));
            sourceStatus = StatusHelper.determineOverallStatus(partitionResults, expectedPartitionCount, "Source", sourceName);

        } finally {
            ConcurrencyUtils.shutdownExecutorService(service, sourceName + "-PartitionExecutor");
        }

        System.out.printf("Finished Data Source %s%n", sourceName);

        // Determine overall failure cause if source failed
        Throwable failureCause = null;
        if (sourceStatus == Status.FAIL) {
            failureCause = partitionResults.stream()
                    .filter(p -> p.status() == Status.FAIL && p.failureCause() != null)
                    .map(PartitionInfo::failureCause)
                    .findFirst()
                    .orElse(new RuntimeException("Source failed due to incomplete partition processing or sub-task failure."));
        }

        return new DataSourceInfo(config.sources().getFirst().sourceId(), sourceName, sourceStatus,
                Duration.between(sourceStart, Instant.now()), currentThreadName, List.copyOf(partitionResults), failureCause);
    }
}
    