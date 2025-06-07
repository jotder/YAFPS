package org.gamma.processing;

import org.gamma.YAFPS; // YAFPF instance might still be needed for non-metrics/non-config methods
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import static org.gamma.metrics.MetricsManager.*; // Import all static members

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionProcessor {

    private final YAFPS YAFPSInstance; // Added to match constructor
    private final EtlPipelineItem config; // Changed field name
    private final String partitionId;
    private final Path partitionPath;

    public PartitionProcessor(YAFPS YAFPSInstance, EtlPipelineItem config, String partitionId, Path partitionPath) { // Added yafpfInstance
        this.YAFPSInstance = YAFPSInstance; // Added assignment
        this.config = config; // Changed assignment
        this.partitionId = partitionId;
        this.partitionPath = partitionPath;
    }

    public PartitionInfo processPartition() { // Use MetricsManager.PartitionInfo (via static import)
        final Instant partitionStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        System.out.printf("%n  Starting Partition %s for %s on T %s%n", this.partitionId, this.config.pipelineName(), threadName);
        SourceItem pollInf = this.config.sources().getFirst();
        List<List<Path>> fileBatches;
        try {
            fileBatches = YAFPS.getFileBatches(this.partitionPath, this.config);
            System.out.printf("    Partition %s: Found %d files matching '%s', %d batches (fileBatchConcurrency=%d).%n",
                    this.partitionId, fileBatches.stream().mapToInt(List::size).sum(), pollInf.fileFilter(), fileBatches.size(), pollInf.numThreads());
        } catch (final IOException e) {
            System.err.printf("!!! FATAL: Failed to list/batch files for Partition %s: %s%n", this.partitionId, e.getMessage());
            return MetricsManager.createFailedPartitionMetrics(pollInf.sourceId(), this.partitionId, e); // Use MetricsManager method
        }

        if (fileBatches.isEmpty()) {
            System.out.printf("    Partition %s: No matching files. Skipping.%n", this.partitionId);
            return new PartitionInfo(pollInf.sourceId(), this.partitionId, Status.PASS, Duration.ZERO, threadName, List.of()); // Use MetricsManager types
        }

        List<BatchInfo> batchResults; // Use MetricsManager.BatchInfo (via static import)
        Status partitionStatus; // Use MetricsManager.Status (via static import)
        final int concurrency = Math.max(1, pollInf.numThreads());
        final ThreadFactory batchFactory = YAFPS.createPlatformThreadFactory(threadName.replace("-Partition-", "-Batch-") + "-");
        final ExecutorService batchExecutor = Executors.newFixedThreadPool(concurrency, batchFactory);
        final List<CompletableFuture<BatchInfo>> batchFutures = new ArrayList<>(); // Use MetricsManager.BatchInfo
        final AtomicInteger batchCounter = new AtomicInteger(1);

        try {
            for (final List<Path> fileBatchData : fileBatches) {
                if (fileBatchData.isEmpty()) continue;
                final int currentBatchId = batchCounter.getAndIncrement();
                // Instantiate BatchProcessor and call its processBatch method
                // BatchProcessor.processBatch() will return CompletableFuture<MetricsManager.BatchInfo>
                org.gamma.processing.BatchProcessor batchProc = new org.gamma.processing.BatchProcessor(this.YAFPSInstance, this.config, currentBatchId, fileBatchData, batchExecutor);
                batchFutures.add(batchProc.processBatch());
            }
            batchResults = new CopyOnWriteArrayList<>(YAFPS.waitForCompletableFuturesAndCollect("Batch", batchFutures, this.partitionId));
            partitionStatus = MetricsManager.determineOverallStatus(batchResults, batchFutures.size(), "Partition", this.partitionId); // Use MetricsManager method
        } finally {
            YAFPS.shutdownExecutorService(batchExecutor, this.partitionId + "-BatchExecutor");
        }
        System.out.printf("  Finished Partition %s for Source %s%n", this.partitionId, pollInf.sourceId());
        return new PartitionInfo(pollInf.sourceId(), this.partitionId, partitionStatus, Duration.between(partitionStart, Instant.now()), threadName, List.copyOf(batchResults)); // Use MetricsManager type
    }
}
