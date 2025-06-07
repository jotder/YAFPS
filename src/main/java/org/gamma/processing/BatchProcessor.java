package org.gamma.processing;

import org.gamma.YAFPF; // YAFPF instance might still be needed for non-metrics/non-config methods
import org.gamma.config.EtlPipelineItem;
import org.gamma.metrics.MetricsManager;
import static org.gamma.metrics.MetricsManager.*; // Import all static members

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections; // Added import
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool; // Required for ForkJoinPool.commonPool()

public class BatchProcessor {

    private final YAFPF yafpfInstance;
    private final EtlPipelineItem config;
    private final int batchId;
    private final List<Path> batchData;
    private final ExecutorService batchExecutor;

    public BatchProcessor(YAFPF yafpfInstance, EtlPipelineItem config, int batchId, List<Path> batchData, ExecutorService batchExecutor) {
        this.yafpfInstance = yafpfInstance; // Available if needed for non-static methods
        this.config = config;
        this.batchId = batchId;
        this.batchData = batchData;
        this.batchExecutor = batchExecutor;
    }

    public CompletableFuture<BatchInfo> processBatch() { // Use MetricsManager.BatchInfo (via static import)
        final String batchNameSuffix = this.batchData.isEmpty() ? "empty" :
                this.batchData.getFirst().getFileName() + (this.batchData.size() > 1 ? ".." + this.batchData.getLast().getFileName() : "");
        final String batchName = "Batch-%d_%s".formatted(this.batchId, batchNameSuffix);

        return CompletableFuture.supplyAsync(
                        () -> {
                            final Instant batchStart = Instant.now();
                            final String currentThreadName = Thread.currentThread().getName();
                            System.out.printf("      %s: Starting processing phase on T %s...%n", batchName, currentThreadName);
                            if (this.batchId % YAFPF.SIMULATED_PROCESSING_FAILURE_MODULO == 0) {
                                throw new RuntimeException("Simulated processing failure in " + batchName);
                            }
                            final Map<String, String> filesToLoad = new LinkedHashMap<>();
                            for (Path p : this.batchData) {
                                filesToLoad.put(p.toString(), "table-" + (p.hashCode() % 2 + 1));
                            }
                            System.out.printf("      %s: Processing phase completed.%n", batchName);
                            return new MetricsManager.ProcessingResult(this.batchId, batchName, batchStart, currentThreadName, this.batchData, filesToLoad, Collections.emptyList()); // Added emptyList for fileInfoList
                        }, this.batchExecutor)
                .thenComposeAsync((MetricsManager.ProcessingResult processingResult) -> { // Explicitly type lambda parameter
                    System.out.printf("      %s: Starting load phase (%d files) virtual threads...%n", processingResult.batchName(), processingResult.filesToLoad().size());
                    final ExecutorService loadExecutor = Executors.newVirtualThreadPerTaskExecutor();
                    final List<LoadTaskContext> loadTaskContexts = new ArrayList<>(); // Use MetricsManager.LoadTaskContext (via static import)
                    try {
                        for (final Map.Entry<String, String> entry : processingResult.filesToLoad().entrySet()) {
                            final String fileName = entry.getKey();
                            final String tableName = entry.getValue();
                            CompletableFuture<LoadInfo> loadFuture = CompletableFuture.supplyAsync( // Use MetricsManager.LoadInfo (via static import)
                                    () -> {
                                        try {
                                            return YAFPF.simulateLoad(fileName, tableName); // YAFPF.simulateLoad now returns MetricsManager.LoadInfo
                                        } catch (final InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new CompletionException("Load interrupted for " + fileName, e);
                                        } catch (final Exception e) {
                                            throw new CompletionException("Load failed for " + fileName, e);
                                        }
                                    }, loadExecutor);
                            loadTaskContexts.add(new LoadTaskContext(fileName, tableName, loadFuture)); // Use MetricsManager.LoadTaskContext (via static import)
                        }

                        final List<CompletableFuture<LoadInfo>> loadFutures = loadTaskContexts.stream() // Use MetricsManager.LoadInfo (via static import)
                                .map(LoadTaskContext::future) // Use MetricsManager.LoadTaskContext (via static import)
                                .toList();

                        return CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[0]))
                                .thenApplyAsync(v ->
                                                buildBatchMetricsFromLoadResults(processingResult, loadTaskContexts), // Call local static method
                                        this.batchExecutor);
                    } finally {
                        CompletableFuture.runAsync(() -> YAFPF.shutdownExecutorService(loadExecutor, processingResult.batchName() + "-LoadExecutor"), ForkJoinPool.commonPool());
                    }
                }, this.batchExecutor)
                .exceptionally(ex -> {
                    System.err.printf("!!! Batch %s failed catastrophically: %s%n", batchName, ex.getMessage());
                    ex.printStackTrace(System.err);
                    return MetricsManager.createFailedBatchMetrics(this.batchId, batchName, ex); // Use MetricsManager method
                });
    }

    private static BatchInfo buildBatchMetricsFromLoadResults( // Return MetricsManager.BatchInfo (via static import)
            MetricsManager.ProcessingResult processingResult, // Explicitly use MetricsManager.ProcessingResult
            List<LoadTaskContext> loadTaskContexts) { // Use MetricsManager.LoadTaskContext (via static import)

        final List<LoadInfo> loadResults = new ArrayList<>(); // Use MetricsManager.LoadInfo (via static import)
        for (LoadTaskContext taskCtx : loadTaskContexts) { // Use MetricsManager.LoadTaskContext (via static import)
            try {
                loadResults.add(taskCtx.future().join());
            } catch (final CompletionException | java.util.concurrent.CancellationException e) {
                System.err.printf("      %s: Load task for file '%s' (table: %s) failed: %s%n",
                        processingResult.batchName(), taskCtx.fileName(), taskCtx.tableName(), e.getMessage());
                Throwable cause = (e instanceof CompletionException) ? e.getCause() : e;
                loadResults.add(MetricsManager.createFailedLoadMetrics(taskCtx.fileName(), taskCtx.tableName(), cause)); // Use MetricsManager method
            }
        }

        final Status loadPhaseStatus = MetricsManager.determineOverallStatus(loadResults, processingResult.filesToLoad().size(), "Load Phase for Batch", processingResult.batchName()); // Use MetricsManager method and type
        final Status overallBatchStatus = (loadPhaseStatus == Status.PASS) ? Status.PASS : Status.FAIL; // Use MetricsManager.Status (via static import)

        if (overallBatchStatus == Status.FAIL) { // Use MetricsManager.Status (via static import)
            System.out.printf("      %s: Load phase marked as FAIL.%n", processingResult.batchName());
        } else {
            System.out.printf("      %s: Load phase completed successfully.%n", processingResult.batchName());
        }
        System.out.printf("    Finished %s on Thread %s%n", processingResult.batchName(), processingResult.threadName());

        return new BatchInfo(processingResult.batchId(), processingResult.batchName(), overallBatchStatus, // Use MetricsManager.BatchInfo (via static import)
                java.time.Duration.between(processingResult.batchStart(), Instant.now()), processingResult.threadName(),
                null,
                List.copyOf(loadResults));
    }
}
