package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.metrics.*;
import org.gamma.util.ConcurrencyUtils;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class BatchProcessor {

    public static final String SIMULATED_PROCESSING_FAILURE_MODULO = "10";

    private final EtlPipelineItem config;
    private final String batchId;
    private final List<Path> batchData;
    private final ExecutorService batchExecutor;

    public BatchProcessor(EtlPipelineItem config, String batchId, List<Path> batchData, ExecutorService batchExecutor) {
        this.config = config;
        this.batchId = batchId;
        this.batchData = batchData;
        this.batchExecutor = batchExecutor;
    }

    public CompletableFuture<BatchInfo> processBatch() {
        final String batchNameSuffix = this.batchData.isEmpty() ? "empty"
                : this.batchData.getFirst().getFileName()
                  + (this.batchData.size() > 1 ? ".." + this.batchData.getLast().getFileName() : "");
        final String batchName = "Batch-%d_%s".formatted(this.batchId, batchNameSuffix);

        return CompletableFuture.supplyAsync(() -> {
            final Instant batchStart = Instant.now();
            final String currentThreadName = Thread.currentThread().getName();
            System.out.printf("      %s: Starting processing phase on T %s...%n", batchName, currentThreadName);

//            if ( SIMULATED_PROCESSING_FAILURE_MODULO == 0) {
//                throw new RuntimeException("Simulated processing failure in " + batchName);
//            }
            final Map<String, String> filesToLoad = new LinkedHashMap<>();
            for (Path p : this.batchData) {
                filesToLoad.put(p.toString(), "table-" + (p.hashCode() % 2 + 1));
            }
            System.out.printf("      %s: Processing phase completed.%n", batchName);
            return new ProcessingResult(this.batchId, batchName, batchStart, currentThreadName,
                    this.batchData, filesToLoad, Collections.emptyList());
        }, this.batchExecutor).thenComposeAsync((ProcessingResult processingResult) -> {
            System.out.printf("      %s: Starting load phase (%d files) virtual threads...%n",
                    processingResult.batchName(), processingResult.filesToLoad().size());
            final ExecutorService loadExecutor = Executors.newVirtualThreadPerTaskExecutor();
            final List<LoadTaskContext> loadTaskContexts = new ArrayList<>();
            try {
                for (final Map.Entry<String, String> entry : processingResult.filesToLoad().entrySet()) {
                    final String fileName = entry.getKey();
                    final String tableName = entry.getValue();
                    CompletableFuture<LoadingInfo> loadFuture = CompletableFuture.supplyAsync(() -> {
                        try {
                            return LoadSimulator.simulateLoad(fileName, tableName);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException("Load interrupted for " + fileName, e);
                        } catch (final Exception e) {
                            throw new CompletionException("Load failed for " + fileName, e);
                        }
                    }, loadExecutor);
                    loadTaskContexts.add(new LoadTaskContext(fileName, tableName, loadFuture));
                }

                final List<CompletableFuture<LoadingInfo>> loadFutures = loadTaskContexts.stream()
                        .map(LoadTaskContext::future).toList();

                return CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[0]))
                        .thenApplyAsync(v ->
                                this.buildBatchMetricsFromLoadResults(processingResult, loadTaskContexts), this.batchExecutor);
            } finally {
                CompletableFuture.runAsync(() -> ConcurrencyUtils
                                .shutdownExecutorService(loadExecutor, processingResult.batchName() + "-LoadExecutor"),
                        ForkJoinPool.commonPool());
            }
        }, this.batchExecutor).exceptionally(ex -> {
            System.err.printf("!!! Batch %s failed catastrophically: %s%n", batchName, ex.getMessage());
            ex.printStackTrace(System.err);
            return StatusHelper.createFailedBatchInfo(this.batchId, batchName, ex);
        });
    }

    private BatchInfo buildBatchMetricsFromLoadResults(ProcessingResult processingResult,
                                                       List<LoadTaskContext> loadTaskContexts) {
        final List<LoadingInfo> loadResults = new ArrayList<>();
        for (LoadTaskContext taskCtx : loadTaskContexts) {
            try {
                loadResults.add(taskCtx.future().join());
            } catch (final CompletionException | java.util.concurrent.CancellationException e) {
                System.err.printf("      %s: Load task for file '%s' (table: %s) failed: %s%n",
                        processingResult.batchName(), taskCtx.fileName(), taskCtx.tableName(), e.getMessage());
                Throwable cause = (e instanceof CompletionException) ? e.getCause() : e;
                loadResults.add(StatusHelper.createFailedLoadInfo(taskCtx.fileName(), taskCtx.tableName(), cause));
            }
        }

        final Status loadPhaseStatus = StatusHelper.determineOverallStatus(loadResults,
                processingResult.filesToLoad().size(), "Load Phase for Batch", processingResult.batchName());
        final Status overallBatchStatus = (loadPhaseStatus == Status.PASS) ? Status.PASS : Status.FAIL;

        if (overallBatchStatus == Status.FAIL) {
            System.out.printf("      %s: Load phase marked as FAIL.%n", processingResult.batchName());
        } else {
            System.out.printf("      %s: Load phase completed successfully.%n", processingResult.batchName());
        }
        System.out.printf("    Finished %s on Thread %s%n", processingResult.batchName(), processingResult.threadName());

        return new BatchInfo(processingResult.batchId(), processingResult.batchName(), overallBatchStatus,
                Duration.between(processingResult.batchStart(), Instant.now()), processingResult.threadName(),
                null, List.copyOf(loadResults));
    }
}
