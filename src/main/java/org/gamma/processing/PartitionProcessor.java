package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.*;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionProcessor {

    private final EtlPipelineItem config;
    private final String partitionId;
    private final Path partitionPath;

    public PartitionProcessor(EtlPipelineItem config, String partitionId, Path partitionPath) {
        this.config = config;
        this.partitionId = partitionId;
        this.partitionPath = partitionPath;
    }

    public PartitionInfo processPartition() {
        final Instant partitionStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        System.out.printf("%n  Starting Partition %s for %s on T %s%n", this.partitionId, this.config.pipelineName(), threadName);
        SourceItem pollInf = this.config.sources().getFirst();
        List<List<Path>> fileBatches;
        try {
            fileBatches = FileUtils.getFileBatches(this.partitionPath, this.config);
            System.out.printf("    Partition %s: Found %d files matching '%s', %d batches (fileBatchConcurrency=%d).%n",
                    this.partitionId, fileBatches.stream().mapToInt(List::size).sum(), pollInf.fileFilter(), fileBatches.size(), pollInf.numThreads());
        } catch (final IOException e) {
            System.err.printf("!!! FATAL: Failed to list/batch files for Partition %s: %s%n", this.partitionId, e.getMessage());
            return StatusHelper.createFailedPartitionInfo(pollInf.sourceId(), this.partitionId, e);
        }

        if (fileBatches.isEmpty()) {
            System.out.printf("    Partition %s: No matching files. Skipping.%n", this.partitionId);
            return new PartitionInfo(pollInf.sourceId(), this.partitionId, Status.PASS, Duration.ZERO, threadName, List.of());
        }

        List<BatchInfo> batchResults;
        Status partitionStatus;
        final int concurrency = Math.max(1, pollInf.numThreads());
        final ThreadFactory batchFactory = ConcurrencyUtils.createPlatformThreadFactory(threadName.replace("-Partition-", "-Batch-") + "-");
        final ExecutorService batchExecutor = Executors.newFixedThreadPool(concurrency, batchFactory);
        final List<CompletableFuture<BatchInfo>> batchFutures = new ArrayList<>();
        final AtomicInteger batchCounter = new AtomicInteger(1);

        try {
            for (final List<Path> fileBatchData : fileBatches) {
                if (fileBatchData.isEmpty())
                    continue;
                final String currentBatchId = "" + batchCounter.getAndIncrement();
                BatchProcessor batchProc = new BatchProcessor(this.config, currentBatchId, fileBatchData, batchExecutor);
                batchFutures.add(batchProc.processBatch());
            }
            batchResults = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Batch", batchFutures, this.partitionId));
            partitionStatus = StatusHelper.determineOverallStatus(batchResults, batchFutures.size(), "Partition", this.partitionId);
        } finally {
            ConcurrencyUtils.shutdownExecutorService(batchExecutor, this.partitionId + "-BatchExecutor");
        }
        System.out.printf("  Finished Partition %s for Source %s%n", this.partitionId, pollInf.sourceId());
        return new PartitionInfo(pollInf.sourceId(), this.partitionId, partitionStatus, Duration.between(partitionStart, Instant.now()), threadName, List.copyOf(batchResults));
    }
}
