package org.gamma.processing;

//import org.gamma.config.YamlSourceConfigAdapter;
import org.gamma.config.EtlPipelineItem;
import org.gamma.datasources.AIRFileParser;
import org.gamma.metrics.*;
import org.gamma.util.ConcurrencyUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Processes a single batch of files, handling both processing and loading phases.
 */
public class BatchHandler {

    private final EtlPipelineItem config;


    public BatchHandler(EtlPipelineItem config) {
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Creates a CompletableFuture representing the processing and subsequent loading
     * of a single batch of files.
     */
    public CompletableFuture<BatchInfo> handle(final String batchId, final List<Path> files, final ExecutorService batchExecutor) throws IOException { // Executor for the processing phase

        final String batchNameSuffix = files.isEmpty() ? "empty" : files.getFirst().getFileName() + (files.size() > 1 ? ".." + files.getLast().getFileName() : "");
        final String batchName = "Batch-%d_%s".formatted(batchId, batchNameSuffix);
        // final Instant batchStartForMetrics = Instant.now(); // No longer needed

        return CompletableFuture.supplyAsync(() -> {
                    // --- Stage 1: Processing Phase (Simulated) ---
                    try {
                        // The return type of processPhase is now ProcessingResult
                        return new AIRFileParser(config).processPhase(batchId, batchName, files);
                    } catch (IOException e) {
                        // Wrap checked IOException in a RuntimeException for CompletableFuture
                        throw new CompletionException(e);
                    }
                }, batchExecutor)
                // --- Stage 2: Load Phase (using Virtual Threads) ---
                .thenComposeAsync(
                        (ProcessingResult parseResults) -> loadPhase(parseResults, batchExecutor),
                        batchExecutor                                        // Executor for the composition step itself
                )
                .exceptionally(ex -> handleProcessingException(ex, batchId, batchName));  // --- Handle Processing Phase Failure ---

        //Todo: backup files, the backup directory tree should be mirror to source directory tree so that duplicate check is easier. files are already in the backup
        // would be considered as processed in previous run
    }


    private CompletableFuture<BatchInfo> loadPhase(ProcessingResult parseResults, ExecutorService completionExecutor) {
        System.out.printf("      %s: Starting load phase (%d files) using virtual threads...%n", parseResults.batchName(), parseResults.filesToLoad().size());

        final ExecutorService loadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        final List<CompletableFuture<LoadingInfo>> loadFutures = new ArrayList<>();

        try {
            for (final Map.Entry<String, String> entry : parseResults.filesToLoad().entrySet()) {
                final String fileName = entry.getKey();
                final String tableName = entry.getValue();
                loadFutures.add(CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return new SimulatedFileLoader().parseFile(fileName, tableName);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException("Load interrupted for " + fileName, e);
                            } catch (Exception e) {
                                throw new CompletionException("Load failed for " + fileName, e);
                            }
                        },
                        loadExecutor
                ));
            }

            return CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[0]))
                    .thenApplyAsync(v -> assembleBatchMetrics(parseResults, loadFutures), completionExecutor);

        } finally {
            // Ensure the load executor is shut down after the loading stage
            // Use runAsync on commonPool to avoid blocking the completionExecutor thread
            CompletableFuture.runAsync(() -> ConcurrencyUtils.shutdownExecutorService(loadExecutor, parseResults.batchName() + "-LoadExecutor"), ForkJoinPool.commonPool());
        }
    }

    private BatchInfo assembleBatchMetrics(ProcessingResult processingResult,
                                           List<CompletableFuture<LoadingInfo>> loadFutures) {
        final List<LoadingInfo> loadResults = new ArrayList<>();
        Throwable firstLoadFailure = null;                    // Capture first failure for potential reporting

        for (CompletableFuture<LoadingInfo> future : loadFutures) {
            try {
                loadResults.add(future.join());
            } catch (CompletionException | CancellationException e) {
                System.err.printf("      %s: Load task failed: %s%n", processingResult.batchName(), e.getMessage());
                Throwable cause = (e instanceof CompletionException) ? e.getCause() : e;
                if (firstLoadFailure == null) firstLoadFailure = cause;

                // Attempt to get original filename/table if possible from exception message
                String failedFileName = "unknown_file";
                if (cause != null && cause.getMessage() != null) {
                    if (cause.getMessage().contains("Load failed for "))
                        failedFileName = cause.getMessage().substring("Load failed for ".length());
                    else if (cause.getMessage().contains("Load interrupted for "))
                        failedFileName = cause.getMessage().substring("Load interrupted for ".length());
                }
                // Add a failed metric placeholder
                loadResults.add(StatusHelper.createFailedLoadInfo(failedFileName, "unknown_table", cause));
            }
        }

        final Status loadPhaseStatus = StatusHelper.determineOverallStatus(loadResults, processingResult.filesToLoad().size(),
                "Load Phase for Batch", processingResult.batchName());
        final Status overallBatchStatus = (loadPhaseStatus == Status.PASS) ? Status.PASS : Status.FAIL;

        if (overallBatchStatus == Status.FAIL)
            System.out.printf("      %s: Load phase marked as FAIL.%n", processingResult.batchName());
        else
            System.out.printf("      %s: Load phase completed successfully.%n", processingResult.batchName());

        System.out.printf("    Finished %s on Thread %s%n", processingResult.batchName(), processingResult.threadName());
        return new BatchInfo(processingResult.batchId(), processingResult.batchName(), overallBatchStatus,
                Duration.between(processingResult.batchStart(), Instant.now()), processingResult.threadName(),
                null,                              // Processing succeeded to get here
                List.copyOf(loadResults));
    }


    private BatchInfo handleProcessingException(Throwable ex, String batchId, String batchName) {
        // Ensure the cause is properly extracted if it's a CompletionException
        final Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
        System.err.printf("      ERROR during processing phase of %s: %s%n", batchName, cause != null ? cause.getMessage() : "Unknown cause");
        // Make sure createFailedBatchMetrics is compatible with the throwable
        return StatusHelper.createFailedBatchInfo(batchId, batchName, cause);
    }
}
    