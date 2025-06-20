package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.BatchInfo;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processes a single partition, managing file batching and execution.
 */
public class PartitionHandler {

    private final EtlPipelineItem config;
    private final String partitionId;
    private final Path path;

    public PartitionHandler(EtlPipelineItem config, String partitionId, Path path) {
        this.config = Objects.requireNonNull(config);
        this.partitionId = Objects.requireNonNull(partitionId);
        this.path = Objects.requireNonNull(path);

    }

    /**
     * Processes the partition. This method is intended to be called asynchronously.
     * Handles internal exceptions and returns metrics.
     */
    public PartitionInfo handle(int partitionSize) {
        final Instant partitionStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        System.out.printf("%n  Starting Partition %s for %s on T-%s%n", partitionId, config.pipelineName(), threadName);

        List<List<Path>> batches;
        SourceItem cnf = config.sources().getFirst();
        try {
            batches = Utils.getFileBatches(path, cnf.fileFilter(), cnf.batchSize());
            System.out.printf("    Partition %s: Found %d files matching '%s', distributed into %d batches (concurrency=%d).%n",
                    partitionId, batches.stream().mapToInt(List::size).sum(), cnf.fileFilter(), batches.size(), cnf.numThreads());
        } catch (IOException e) {
            System.err.printf("!!! FATAL: Failed to list/batch files for Partition %s: %s%n", partitionId, e.getMessage());
            return StatusHelper.createFailedPartitionInfo(cnf.sourceId(), partitionId, e);
        }

        if (batches.isEmpty()) {
            System.out.printf("    Partition %s: No matching files found to process. Skipping.%n", partitionId);
            return new PartitionInfo(cnf.sourceId(), partitionId, Status.PASS, Duration.ZERO, threadName, List.of());
        }

        List<BatchInfo> results;
        Status status;
        int expectedBatchCount = batches.size();

        int threads;
        if (partitionSize == 1) // in case files are not separated in directories for parallel execution. running each bucket as partition,
            threads = cnf.numThreads();
        else
            threads = 1; // as partitions are executed in parallel, let buckets run one by one

        ThreadFactory factory = ConcurrencyUtils.createPlatformThreadFactory(threadName.replace("-Part-", "-Batch-") + "-");
        ExecutorService service = Executors.newFixedThreadPool(threads, factory);
        List<CompletableFuture<BatchInfo>> futures = new ArrayList<>();
        AtomicInteger batchCounter = new AtomicInteger(1);

        try {
            for (List<Path> batch : batches) {
                if (batch.isEmpty()) {
                    expectedBatchCount--; // Adjust count if a batch was unexpectedly empty
                    continue;
                }
                String batchId = "" + batchCounter.getAndIncrement();

                BatchHandler processor = new BatchHandler(config);

                CompletableFuture<BatchInfo> batchFuture = null;
                try {
                    batchFuture = processor.handle(batchId, batch, service);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (batchFuture != null)
                    futures.add(batchFuture);
            }

            // Wait for batches and collect results, Use CopyOnWriteArrayList if modifying the list after creation, otherwise ArrayList is fine
            results = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Batch", futures, partitionId));

            // Determine overall partition status
            status = StatusHelper.determineOverallStatus(results, expectedBatchCount, "Partition", partitionId);

        } finally {
            ConcurrencyUtils.shutdownExecutorService(service, partitionId + "-BatchExecutor");
        }

        System.out.printf("  Finished Partition %s for Source %s%n", partitionId, cnf.sourceId());
        // Determine if there was an overall failure not captured by individual batch metrics (e.g., executor issue)
        Throwable failureCause = (status == Status.FAIL && results.stream().noneMatch(b -> b.status() == Status.FAIL))
                ? new RuntimeException("Partition failed due to incomplete batch processing.")
                : null; // Or try to find the first batch failure cause

        // Find the first batch failure cause if the partition failed
        if (status == Status.FAIL && failureCause == null) {
            failureCause = results.stream()
                    .filter(b -> b.status() == Status.FAIL && b.failureCause() != null)
                    .map(BatchInfo::failureCause)
                    .findFirst()
                    .orElse(null); // Or keep the generic message above
        }

        return new PartitionInfo(cnf.sourceId(), partitionId, status, Duration.between(partitionStart,
                Instant.now()), threadName, List.copyOf(results), failureCause);
    }
}
    