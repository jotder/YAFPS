package org.gamma.processing;

import org.gamma.config.YamlSourceConfigAdapter;
import org.gamma.metrics.DataSourceMetrics;
import org.gamma.metrics.StatusHelper;
import org.gamma.metrics.PartitionMetrics;
import org.gamma.metrics.Status;
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

    private final YamlSourceConfigAdapter config;

    public DataSourceHandler(YamlSourceConfigAdapter config) {
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Processes the data source. Intended to be called asynchronously.
     * Handles locking and manages partition processing.
     */
    public DataSourceMetrics process() throws IOException {
        final Instant sourceStart = Instant.now();
        final String sourceName = "%s".formatted(config.sourceName());
        final String currentThreadName = Thread.currentThread().getName();
        final Path sourceDirPath = config.sourceDir();
        System.out.printf("%nProcessing Data Source : %s on Thread %s%n", sourceName, currentThreadName);

        List<Path> partitions;
        try {
            if (config.isDirAsPartition()) {
                partitions = Utils.getDirectoriesAsPartition(config.sourceDir(), config.dirFilter());
                System.out.printf("  Source %s: Found %d partition directories matching '%s'.%n",
                        sourceName, partitions.size(), config.dirFilter());
            } else {
                partitions = List.of(sourceDirPath);
                System.out.printf("  Source %s: Processing source directory '%s' as a single partition.%n", sourceName, config.sourceDir());
            }
        } catch (IOException e) {
            System.err.printf("!!! FATAL: Failed to list partitions for Source %s: %s%n", sourceName, e.getMessage());
            return StatusHelper.createFailedDataSourceMetrics(config, e);
        }

        if (partitions.isEmpty() && config.isDirAsPartition()) {
            System.out.printf("  Source %s: No matching partition directories found. Skipping.%n", sourceName);
            return new DataSourceMetrics(config.sourceID(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of());
        }
        System.out.printf("  Source %s: Submitting %d partitions (batchSize=%d).%n", sourceName, partitions.size(), config.batchSize());

        List<PartitionMetrics> partitionResults;
        Status sourceStatus;
        int expectedPartitionCount = partitions.size();

        final ExecutorService service = Executors.newFixedThreadPool(config.maxConcurrency(),
                ConcurrencyUtils.createPlatformThreadFactory(sourceName + "-partition-"));

        List<CompletableFuture<PartitionMetrics>> partitionFutures = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(1);
        try {
            for (Path path : partitions) {
                String partitionId = path.getFileName() + "_" + counter.getAndIncrement();

                PartitionHandler handler = new PartitionHandler(config, partitionId, path);
                CompletableFuture<PartitionMetrics> partitionFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return handler.handle(partitions.size()); // Call the process method
                            } catch (RuntimeException e) {  // Catch unexpected runtime exceptions from process()
                                System.err.printf("! Exception processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                e.printStackTrace(System.err);
                                // Create failed metrics if the processor itself threw unexpectedly
                                return StatusHelper.createFailedPartitionMetrics(config.sourceID(), partitionId, e);
                            }
                        },
                        service
                );
                partitionFutures.add(partitionFuture);
            }

            partitionResults = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Partition",
                    partitionFutures, config.sourceID()));
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
                    .map(PartitionMetrics::failureCause)
                    .findFirst()
                    .orElse(new RuntimeException("Source failed due to incomplete partition processing or sub-task failure."));
        }

        return new DataSourceMetrics(config.sourceID(), sourceName, sourceStatus,
                Duration.between(sourceStart, Instant.now()), currentThreadName, List.copyOf(partitionResults), failureCause);
    }
}
    