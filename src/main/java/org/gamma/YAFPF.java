package org.gamma;

// Assume these classes exist in the com.gamma.config package
// and would be imported if FFPF.java is in a different package,
// or directly accessible if FFPF is also in com.gamma.config.
// For this example, we'll use fully qualified names or assume appropriate imports.

import org.gamma.config.AppConfig;
import org.gamma.config.ConfigManager;
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import static org.gamma.metrics.MetricsManager.*; // Import all static members

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// ToDo
// 1. run single instance only, (on each dir only) V
// 2. create a 0 kb / full backup, also use for duplicate check
// 3. generate .tmp while writing for output file, rename after closing writer,
// 4. append status without writing headers nor subsequent run
// 5. Load in parallel
// 6. Add post loading hook
// 7. consider re processing scenarios
// 8. Add configurations (Now moving towards external YAML via ConfigManager)
// 9. process sub directories / files


/**
 * Processes data sources, partitions, batches, and loads files concurrently
 * using CompletableFuture and ExecutorService.
 * Uses Virtual Threads for the final loading phase within each batch.
 * Configuration is expected to be provided as a List of EtlPipelineItem objects,
 * typically loaded by a ConfigManager from a YAML file.
 */
public class YAFPF {

    // --- Constants ---
    public static final int SIMULATED_PROCESSING_FAILURE_MODULO = 10; // Fail processing every Nth batch
    public static final int SIMULATED_LOAD_FAILURE_MODULO = 5;      // Fail load pseudo-randomly based on hash
    public static final Duration SIMULATED_LOAD_DURATION = Duration.ofMillis(150); // Reduced sleep for demo
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(60); // This can remain private

    // --- Configuration Data Holder (Hypothetical - would be in com.gamma.config.EtlPipelineItem) ---
    // This is a placeholder to illustrate the structure FFPF now expects.
    // In a real setup, FFPF would import com.gamma.config.EtlPipelineItem.

    // Metrics-related records and enums have been moved to MetricsManager.java

    // --- Class Members ---
    private final AppConfig sourceConfigs; // Uses the new EtlPipelineItem

    /**
     * Constructor for the executor.
     *
     * @param sourceConfigs List of configurations for the data sources to process.
     *                      These are instances of {@link AppConfig}, typically loaded
     *                      by a {@code com.gamma.config.ConfigManager}.
     */
    public YAFPF(final AppConfig sourceConfigs) {
        this.sourceConfigs = Objects.requireNonNull(sourceConfigs, "Data source configurations cannot be null");
        if (sourceConfigs.etlPipelines().isEmpty()) {
            System.err.println("Warning: No source configurations provided.");
        }
    }

    /**
     * Gets the list of source configurations.
     *
     * @return The list of {@link EtlPipelineItem} objects.
     */
    List<EtlPipelineItem> getSources() {
        return sourceConfigs.etlPipelines();
    }

    // --- Entry Point ---
    public MetricsManager.ExecutionInfo execute() throws InterruptedException { // Use MetricsManager type
        final Instant executionStart = Instant.now();
        final List<MetricsManager.DataSourceInfo> finalDataSourceMetrics; // Use MetricsManager type

        final int dataSourceConcurrency = Math.max(1, getSources().size());
        final ThreadFactory dataSourceFactory = createPlatformThreadFactory("DataSource-");
        final ExecutorService dataSourceExecutor = Executors.newFixedThreadPool(dataSourceConcurrency, dataSourceFactory);
        final List<CompletableFuture<MetricsManager.DataSourceInfo>> dataSourceFutures = new ArrayList<>(); // Use MetricsManager type

        System.out.printf("Starting execution with %d concurrent data sources.%n", dataSourceConcurrency);

        try {
            for (final EtlPipelineItem config : getSources()) { // Iterates over EtlPipelineItem
                final CompletableFuture<MetricsManager.DataSourceInfo> dataSourceFuture = CompletableFuture.supplyAsync( // Use MetricsManager type
                        () -> {
                            try {
                                return processDataSource(config); // Passes EtlPipelineItem
                            } catch (final RuntimeException e) {
                                System.err.printf("!!! Uncaught RuntimeException processing data source %s: %s%n", config.pipelineName(), e.getMessage());
                                e.printStackTrace(System.err);
                                return MetricsManager.createFailedDataSourceMetrics(config, e); // Use MetricsManager method
                            }
                        },
                        dataSourceExecutor
                );
                dataSourceFutures.add(dataSourceFuture);
            }
            finalDataSourceMetrics = new CopyOnWriteArrayList<>(waitForCompletableFuturesAndCollect("DataSource", dataSourceFutures, null));
        } finally {
            shutdownExecutorService(dataSourceExecutor, "DataSourceExecutor");
        }

        System.out.println("All data source tasks completed or failed.");
        return new MetricsManager.ExecutionInfo(Duration.between(executionStart, Instant.now()), List.copyOf(finalDataSourceMetrics)); // Use MetricsManager type
    }

    // TODO: Move this method to DataSourceProcessor
    private MetricsManager.DataSourceInfo processDataSource(final EtlPipelineItem conf) { // Use MetricsManager type
        final Instant sourceStart = Instant.now();
        final String sourceName = "%s".formatted(conf.pipelineName());
        final String currentThreadName = Thread.currentThread().getName();
        System.out.printf("%nProcessing Data Source : %s on Thread %s%n", sourceName, currentThreadName);
        SourceItem pollInf = conf.sources().getFirst();
        final Path sourceDirPath = pollInf.sourceDir();
        final Path lockFilePath = sourceDirPath.resolve(".fast_executor.lock");

        try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel();
             FileLock lock = channel.tryLock()) {

            if (lock == null) {
                System.err.printf("!!! WARN: Could not acquire lock for Source %s (%s). Already processed? Skipping.%n", sourceName, lockFilePath);
                return new MetricsManager.DataSourceInfo(conf.pipelineName(), sourceName, MetricsManager.Status.PASS, Duration.ZERO, currentThreadName, List.of(), // Use MetricsManager types
                        new RuntimeException("Skipped due to existing lock file: " + lockFilePath));
            }
            System.out.printf("  Source %s: Acquired lock file %s%n", sourceName, lockFilePath);

            List<Path> partitionsToProcess;
            try {
                // Use the new helper method
                partitionsToProcess = discoverPartitions(conf, sourceName);
            } catch (final IOException e) {
                System.err.printf("!!! FATAL: Failed to list/discover partitions for Source %s: %s%n", sourceName, e.getMessage());
                // Ensure lock is released if we return early due to this error. The try-with-resources on the lock handles this, but good to be mindful.
                return MetricsManager.createFailedDataSourceMetrics(conf, e); // Use MetricsManager method
            }

            if (partitionsToProcess.isEmpty() && pollInf.useSubDirAsPartition()) { // Check if dirAsPartition was true for this message
                System.out.printf("  Source %s: No matching partition directories found. Skipping.%n", sourceName);
                return new MetricsManager.DataSourceInfo(conf.pipelineName(), sourceName, MetricsManager.Status.PASS, Duration.ZERO, currentThreadName, List.of()); // Use MetricsManager types
            }

            System.out.printf("  Source %s: Submitting %d partitions (partitionConcurrency=%d).%n", sourceName, partitionsToProcess.size(), pollInf.batchSize()); // conf.batchSize() is partitionConcurrency

            List<MetricsManager.PartitionInfo> partitionResults; // Use MetricsManager type
            MetricsManager.Status sourceStatus; // Use MetricsManager type
            final int partitionConcurrency = Math.max(1, pollInf.batchSize()); // Uses conf.batchSize()
            final ThreadFactory partitionFactory = createPlatformThreadFactory(sourceName + "-Partition-");
            final ExecutorService partitionExecutor = Executors.newFixedThreadPool(partitionConcurrency, partitionFactory);
            final List<CompletableFuture<MetricsManager.PartitionInfo>> partitionFutures = new ArrayList<>(); // Use MetricsManager type
            final AtomicInteger partitionCounter = new AtomicInteger(1);

            try {
                for (final Path partitionPath : partitionsToProcess) {
                    final String partitionId = partitionPath.getFileName() + "_" + partitionCounter.getAndIncrement();
                    partitionFutures.add(CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return processPartition(conf, partitionId, partitionPath); // Passes EtlPipelineItem
                                } catch (final RuntimeException e) {
                                    System.err.printf("!!! Uncaught RuntimeException processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                    e.printStackTrace(System.err);
                                    return MetricsManager.createFailedPartitionMetrics(pollInf.sourceId(), partitionId, e); // Use MetricsManager method
                                }
                            }, partitionExecutor));
                }
                partitionResults = new CopyOnWriteArrayList<>(waitForCompletableFuturesAndCollect("Partition", partitionFutures, pollInf.sourceId()));
                sourceStatus = MetricsManager.determineOverallStatus(partitionResults, partitionsToProcess.size(), "Source", sourceName); // Use MetricsManager method
            } finally {
                shutdownExecutorService(partitionExecutor, sourceName + "-PartitionExecutor");
            }
            System.out.printf("Finished Data Source %s%n", sourceName);
            return new MetricsManager.DataSourceInfo(pollInf.sourceId(), sourceName, sourceStatus, Duration.between(sourceStart, Instant.now()), currentThreadName, List.copyOf(partitionResults)); // Use MetricsManager type
        } catch (OverlappingFileLockException e) {
            System.err.printf("!!! WARN: Lock for Source %s (%s) held by another process. Skipping. %s%n", sourceName, lockFilePath, e.getMessage());
            return new MetricsManager.DataSourceInfo(pollInf.sourceId(), sourceName, MetricsManager.Status.PASS, Duration.ZERO, currentThreadName, List.of(), // Use MetricsManager types
                    new RuntimeException("Skipped due to overlapping lock: " + lockFilePath, e));
        } catch (IOException e) {
            System.err.printf("!!! FATAL: Failed to access lock file for Source %s (%s): %s%n", sourceName, lockFilePath, e.getMessage());
            return MetricsManager.createFailedDataSourceMetrics(conf, new IOException("Failed to acquire lock file: " + lockFilePath, e)); // Use MetricsManager method
        } finally {
            System.out.printf("  Source %s: Released lock file %s%n", sourceName, lockFilePath);
        }
    }

    // --- Partition Level Processing ---
    // TODO: Move this method to PartitionProcessor
    public MetricsManager.PartitionInfo processPartition(final EtlPipelineItem conf, final String partitionId, final Path partitionPath) { // Use MetricsManager type
        final Instant partitionStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        System.out.printf("%n  Starting Partition %s for %s on T %s%n", partitionId, conf.pipelineName(), threadName);
        SourceItem pollInf = conf.sources().getFirst(); //todo
        List<List<Path>> fileBatches;
        try {
            fileBatches = getFileBatches(partitionPath, conf); // Passes EtlPipelineItem
            System.out.printf("    Partition %s: Found %d files matching '%s', %d batches (fileBatchConcurrency=%d).%n",
                    partitionId, fileBatches.stream().mapToInt(List::size).sum(), pollInf.fileFilter(), fileBatches.size(), pollInf.numThreads()); // Uses conf.concurrency()
        } catch (final IOException e) {
            System.err.printf("!!! FATAL: Failed to list/batch files for Partition %s: %s%n", partitionId, e.getMessage());
            return MetricsManager.createFailedPartitionMetrics(pollInf.sourceId(), partitionId, e); // Use MetricsManager method
        }

        if (fileBatches.isEmpty()) {
            System.out.printf("    Partition %s: No matching files. Skipping.%n", partitionId);
            return new MetricsManager.PartitionInfo(pollInf.sourceId(), partitionId, MetricsManager.Status.PASS, Duration.ZERO, threadName, List.of()); // Use MetricsManager types
        }

        List<MetricsManager.BatchInfo> batchResults; // Use MetricsManager type
        MetricsManager.Status partitionStatus; // Use MetricsManager type
        final int concurrency = Math.max(1, pollInf.numThreads()); // Uses conf.concurrency()
        final ThreadFactory batchFactory = createPlatformThreadFactory(threadName.replace("-Partition-", "-Batch-") + "-");
        final ExecutorService batchExecutor = Executors.newFixedThreadPool(concurrency, batchFactory);
        final List<CompletableFuture<MetricsManager.BatchInfo>> batchFutures = new ArrayList<>(); // Use MetricsManager type
        final AtomicInteger batchCounter = new AtomicInteger(1);

        try {
            for (final List<Path> fileBatchData : fileBatches) {
                if (fileBatchData.isEmpty()) continue;
                final int currentBatchId = batchCounter.getAndIncrement();
                batchFutures.add(processBatch(conf, currentBatchId, fileBatchData, batchExecutor)); // Passes EtlPipelineItem
            }
            batchResults = new CopyOnWriteArrayList<>(waitForCompletableFuturesAndCollect("Batch", batchFutures, partitionId));
            partitionStatus = MetricsManager.determineOverallStatus(batchResults, batchFutures.size(), "Partition", partitionId); // Use MetricsManager method
        } finally {
            shutdownExecutorService(batchExecutor, partitionId + "-BatchExecutor");
        }
        System.out.printf("  Finished Partition %s for Source %s%n", partitionId, pollInf.sourceId());
        return new MetricsManager.PartitionInfo(pollInf.sourceId(), partitionId, partitionStatus, Duration.between(partitionStart, Instant.now()), threadName, List.copyOf(batchResults)); // Use MetricsManager type
    }

    // --- Batch Level Processing (Returns CompletableFuture) ---
    // Records moved to be public static above

    // Add this private method to the FFPF class
    public static List<Path> discoverPartitions(EtlPipelineItem conf, String sourceName) throws IOException {
        SourceItem pollInf = conf.sources().getFirst();
        Path sourceDirPath = pollInf.sourceDir();
        if (!Files.isDirectory(sourceDirPath)) {
            throw new IOException("Source directory does not exist: " + sourceDirPath);
        }

        if (pollInf.useSubDirAsPartition()) {
            List<Path> foundPartitions = getDirectoriesAsPartition(pollInf.sourceDir(), pollInf.dirFilter());
            System.out.printf("  Source %s: Found %d partitions matching '%s'.%n", sourceName, foundPartitions.size(), pollInf.dirFilter());
            return foundPartitions;
        } else {
            System.out.printf("  Source %s: Processing '%s' as single partition.%n", sourceName, pollInf.sourceDir());
            return List.of(sourceDirPath);
        }
    }

    // TODO: Move this method to BatchProcessor
    private static CompletableFuture<MetricsManager.BatchInfo> processBatch( // Changed to private, Use MetricsManager type
            final EtlPipelineItem conf, // Parameter is EtlPipelineItem
            final int batchId,
            final List<Path> batchData,
            final ExecutorService batchExecutor) {

        final String batchNameSuffix = batchData.isEmpty() ? "empty" :
                batchData.getFirst().getFileName() + (batchData.size() > 1 ? ".." + batchData.getLast().getFileName() : "");
        final String batchName = "Batch-%d_%s".formatted(batchId, batchNameSuffix);

        return CompletableFuture.supplyAsync(
                        () -> {
                            final Instant batchStart = Instant.now();
                            final String currentThreadName = Thread.currentThread().getName();
                            System.out.printf("      %s: Starting processing phase on T %s...%n", batchName, currentThreadName);
                            if (batchId % SIMULATED_PROCESSING_FAILURE_MODULO == 0) {
                                throw new RuntimeException("Simulated processing failure in " + batchName);
                            }
                            final Map<String, String> filesToLoad = new LinkedHashMap<>();
                            for (Path p : batchData) {
                                filesToLoad.put(p.toString(), "table-" + (p.hashCode() % 2 + 1));
                            }
                            System.out.printf("      %s: Processing phase completed.%n", batchName);
                            return new MetricsManager.ProcessingResult(batchId, batchName, batchStart, currentThreadName, batchData, filesToLoad); // Use MetricsManager type
                        }, batchExecutor)
                .thenComposeAsync(processingResult -> {
                    System.out.printf("      %s: Starting load phase (%d files) virtual threads...%n", processingResult.batchName(), processingResult.filesToLoad().size());
                    final ExecutorService loadExecutor = Executors.newVirtualThreadPerTaskExecutor();
                    // Use LoadTaskContext to keep fileName and tableName with the future
                    final List<MetricsManager.LoadTaskContext> loadTaskContexts = new ArrayList<>(); // Use MetricsManager type
                    try {
                        for (final Map.Entry<String, String> entry : processingResult.filesToLoad().entrySet()) {
                            final String fileName = entry.getKey();
                            final String tableName = entry.getValue();
                            CompletableFuture<MetricsManager.LoadInfo> loadFuture = CompletableFuture.supplyAsync( // Use MetricsManager type
                                    () -> {
                                        try {
                                            return simulateLoad(fileName, tableName); // simulateLoad still returns YAFPF.LoadInfo or needs update
                                        } catch (final InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            throw new CompletionException("Load interrupted for " + fileName, e);
                                        } catch (final Exception e) {
                                            // Wrap other exceptions to be handled by the CompletionStage
                                            throw new CompletionException("Load failed for " + fileName, e);
                                        }
                                    }, loadExecutor);
                            loadTaskContexts.add(new MetricsManager.LoadTaskContext(fileName, tableName, loadFuture)); // Use MetricsManager type
                        }

                        final List<CompletableFuture<MetricsManager.LoadInfo>> loadFutures = loadTaskContexts.stream() // Use MetricsManager type
                                .map(MetricsManager.LoadTaskContext::future) // Use MetricsManager type
                                .toList();

                        return CompletableFuture.allOf(loadFutures.toArray(new CompletableFuture[0]))
                                .thenApplyAsync(v ->
                                                // Extracted logic into a new helper method
                                                buildBatchMetricsFromLoadResults(processingResult, loadTaskContexts), // buildBatchMetricsFromLoadResults returns YAFPF.BatchInfo or needs update
                                        batchExecutor); // Ensure this runs on batchExecutor
                    } finally {
                        // Ensure loadExecutor is always shutdown, even if task submission fails
                        CompletableFuture.runAsync(() -> shutdownExecutorService(loadExecutor, processingResult.batchName() + "-LoadExecutor"), ForkJoinPool.commonPool());
                    }
                }, batchExecutor);
    }

    private static MetricsManager.BatchInfo buildBatchMetricsFromLoadResults( // Changed to private, Use MetricsManager type
            MetricsManager.ProcessingResult processingResult, // Use MetricsManager type
            List<MetricsManager.LoadTaskContext> loadTaskContexts) { // Use MetricsManager type

        final List<MetricsManager.LoadInfo> loadResults = new ArrayList<>(); // Use MetricsManager type
        for (MetricsManager.LoadTaskContext taskCtx : loadTaskContexts) { // Use MetricsManager type
            try {
                loadResults.add(taskCtx.future().join());
            } catch (final CompletionException | CancellationException e) {
                // Log with more context
                System.err.printf("      %s: Load task for file '%s' (table: %s) failed: %s%n",
                        processingResult.batchName(), taskCtx.fileName(), taskCtx.tableName(), e.getMessage());
                Throwable cause = (e instanceof CompletionException) ? e.getCause() : e;
                // Now we have accurate fileName and tableName
                loadResults.add(MetricsManager.createFailedLoadMetrics(taskCtx.fileName(), taskCtx.tableName(), cause)); // Use MetricsManager method
            }
        }

        final MetricsManager.Status loadPhaseStatus = MetricsManager.determineOverallStatus(loadResults, processingResult.filesToLoad().size(), "Load Phase for Batch", processingResult.batchName()); // Use MetricsManager method and type
        // The overallBatchStatus should consider if the processing phase (before load) also passed.
        // Assuming if we reached here, processing phase was successful, otherwise .exceptionally() would have caught it.
        // If processingResult itself could carry a status, that would be more explicit.
        // For now, if loadPhaseStatus is FAIL, the batch is FAIL.
        final MetricsManager.Status overallBatchStatus = (loadPhaseStatus == MetricsManager.Status.PASS) ? MetricsManager.Status.PASS : MetricsManager.Status.FAIL; // Use MetricsManager type

        if (overallBatchStatus == MetricsManager.Status.FAIL) { // Use MetricsManager type
            System.out.printf("      %s: Load phase marked as FAIL.%n", processingResult.batchName());
        } else {
            System.out.printf("      %s: Load phase completed successfully.%n", processingResult.batchName());
        }
        System.out.printf("    Finished %s on Thread %s%n", processingResult.batchName(), processingResult.threadName());

        return new MetricsManager.BatchInfo(processingResult.batchId(), processingResult.batchName(), overallBatchStatus, // Use MetricsManager type
                Duration.between(processingResult.batchStart(), Instant.now()), processingResult.threadName(),
                null, // This null is for processing phase failure, handled by .exceptionally()
                List.copyOf(loadResults));
    }


    // --- Load Task Simulation ---
    // This method's return type and internal new LoadInfo need to be updated to MetricsManager.LoadInfo
    public static MetricsManager.LoadInfo simulateLoad(final String fileName, final String targetTable) throws InterruptedException { // Use MetricsManager type
        final Instant loadStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        final String stageName = "Loading " + Paths.get(fileName).getFileName() + " to " + targetTable;
        try {
            Thread.sleep(SIMULATED_LOAD_DURATION);
            if (fileName.hashCode() % SIMULATED_LOAD_FAILURE_MODULO == 0) {
                throw new RuntimeException("Simulated DB error for " + fileName);
            }
            System.out.printf("        -> %s completed on %s%n", stageName, threadName);
            return new MetricsManager.LoadInfo(fileName, targetTable, MetricsManager.Status.PASS, Duration.between(loadStart, Instant.now()), threadName, null); // Use MetricsManager types
        } catch (final RuntimeException e) {
            System.err.printf("          ERROR during %s on %s: %s%n", stageName, threadName, e.getMessage());
            throw e;
        } catch (final InterruptedException e) {
            System.err.printf("          INTERRUPTED during %s on %s%n", stageName, threadName);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    public static <T extends MetricsManager.HasStatus> List<T> waitForCompletableFuturesAndCollect( // Use MetricsManager type
            final String levelName, final List<CompletableFuture<T>> futures, final Object identifier) {
        if (futures.isEmpty()) {
            System.out.printf("      No %s tasks for (ID: %s).%n", levelName, identifier != null ? identifier : "N/A");
            return Collections.emptyList();
        }
        final List<T> results = new ArrayList<>();
        final String idStr = identifier != null ? identifier.toString() : "N/A";
        System.out.printf("      Waiting for %d %s tasks (ID: %s)...%n", futures.size(), levelName, idStr);
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            System.out.printf("      All %s tasks (ID: %s) completed. Collecting...%n", levelName, idStr);
        } catch (final CancellationException e) {
            System.err.printf("!!! %s waiting (allOf) cancelled (ID: %s).%n", levelName, idStr);
        } catch (final CompletionException e) {
            System.err.printf("!!! Error during allOf completion for %s (ID: %s): %s%n", levelName, idStr, e.getMessage());
        }
        for (final CompletableFuture<T> future : futures) {
            try {
                results.add(future.join());
            } catch (final CompletionException e) {
                System.err.printf("!!! %s task (ID: %s) failed: %s%n", levelName, idStr, e.getCause());
            } catch (final CancellationException e) {
                System.err.printf("!!! %s task (ID: %s) cancelled.%n", levelName, idStr);
            }
        }
        System.out.printf("      Finished waiting for %s (ID: %s). %d results (of %d submitted).%n",
                levelName, idStr, results.size(), futures.size());
        return results;
    }

    // determineOverallStatus has been moved to MetricsManager.java

    // --- Factory and Shutdown Methods ---
    public static ThreadFactory createPlatformThreadFactory(final String prefix) { // This is a generic utility, not directly metrics related for now
        return Thread.ofPlatform().name(prefix, 0).factory();
    }

    public static void shutdownExecutorService(final ExecutorService executor, final String name) {
        if (executor == null) return;
        System.out.printf("      Shutting down executor: %s%n", name);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                System.err.printf("      Executor %s timeout, forcing shutdown...%n", name);
                List<Runnable> dropped = executor.shutdownNow();
                System.err.printf("      Executor %s forced. Dropped %d tasks.%n", name, dropped.size());
                if (!executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                    System.err.printf("      Executor %s did not terminate after force.%n", name);
                } else System.out.printf("      Executor %s terminated after force.%n", name);
            } else System.out.printf("      Executor %s terminated gracefully.%n", name);
        } catch (final InterruptedException ie) {
            System.err.printf("      Shutdown for %s interrupted. Forcing now.%n", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // --- File System Utilities ---
    public static List<Path> getDirectoriesAsPartition(final Path baseDir, final String dirFilter) throws IOException {
        if (!Files.isDirectory(baseDir)) throw new IOException("Base dir not found: " + baseDir);
        final PathMatcher dirMatcher = (dirFilter != null && !dirFilter.isBlank())
                ? FileSystems.getDefault().getPathMatcher(dirFilter) : path -> true;
        try (var stream = Files.list(baseDir)) {
            return stream.filter(Files::isDirectory).filter(p -> dirMatcher.matches(p.getFileName())).toList();
        }
    }

    public static List<Path> listFiles(final Path sourceDir, final String fileFilter) throws IOException {
        if (!Files.isDirectory(sourceDir)) {
            System.err.printf("Warning: Dir not found: %s. Empty list.%n", sourceDir);
            return Collections.emptyList();
        }
        final PathMatcher fileMatcher = (fileFilter != null && !fileFilter.isBlank())
                ? FileSystems.getDefault().getPathMatcher(fileFilter) : path -> true;
        try (var stream = Files.list(sourceDir)) {
            return stream.filter(Files::isRegularFile).filter(p -> fileMatcher.matches(p.getFileName())).toList();
        }
    }

    public static List<List<Path>> getFileBatches(final Path partitionPath, final EtlPipelineItem conf) throws IOException { // Parameter is EtlPipelineItem
        SourceItem pollInf = conf.sources().getFirst();
        final List<Path> pathList = listFiles(partitionPath, pollInf.fileFilter());
        final int numBuckets = Math.max(1, pollInf.numThreads()); // Uses conf.concurrency()
        if (pathList.isEmpty()) return Collections.emptyList();
        final List<List<Path>> buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) buckets.add(new ArrayList<>());
        final AtomicInteger bucketIndex = new AtomicInteger(0);
        pathList.forEach(p -> buckets.get(bucketIndex.getAndIncrement() % numBuckets).add(p));
        buckets.removeIf(List::isEmpty);
        return buckets;
    }

    // Failure Metric Helper methods have been moved to MetricsManager.java

    // --- Main Method (Example Usage) ---
    public static void main(final String[] args) throws IOException {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        // --- Configuration Loading (Illustrative) ---
        // In a real application, you would use com.gamma.config.ConfigManager here:
        //
        // com.gamma.config.ConfigManager configManager = new com.gamma.config.ConfigManager("path/to/your/config.yaml");
        // List<EtlPipelineItem> configs = configManager.loadEtlPipelineItemurations();
        //
        // For this example, we'll manually create EtlPipelineItem objects.
        AppConfig appConfig = ConfigManager.getConfig();
        for (EtlPipelineItem pipelineItem : appConfig.etlPipelines()) {
            String pipelineName = pipelineItem.pipelineName();
        }

        createDummyData(appConfig.etlPipelines()); // Pass List<EtlPipelineItem>

        final YAFPF executor = new YAFPF(appConfig); // Pass List<EtlPipelineItem>

        try {
            System.out.println("========================================================");
            System.out.println(" Starting Execution (CompletableFuture with Externalized Config Model) ");
            System.out.println("========================================================");
            final MetricsManager.ExecutionInfo metrics = executor.execute(); // Use MetricsManager type
            System.out.println("\n\n========================================================");
            System.out.println(" Execution Finished ");
            System.out.println("========================================================");
            printMetricsSummary(metrics);
        } catch (final InterruptedException e) {
            System.err.println("\n>>> Main execution thread interrupted.");
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
            System.err.println("\n>>> An unexpected error occurred during main execution:");
            e.printStackTrace();
        }
    }

    // --- Metrics Printing ---
    private static void printMetricsSummary(final MetricsManager.ExecutionInfo metrics) { // Use MetricsManager type
        System.out.println("Total Execution Time: " + metrics.totalDuration().toMillis() + " ms");
        System.out.println("---------------------- METRICS SUMMARY ----------------------");
        for (final MetricsManager.DataSourceInfo dsMetrics : metrics.dataSourceInfo()) { // Use MetricsManager type
            String dsFailInfo = dsMetrics.failureCause() != null ? "[FAIL: " + dsMetrics.failureCause().getMessage() + "]" : "";
            System.out.printf("Data Source: %-10s | Status: %-4s | Duration: %5dms | Thread: %-20s | Partitions: %d %s%n",
                    dsMetrics.sourceName(), dsMetrics.status(), dsMetrics.duration().toMillis(), dsMetrics.threadName(), dsMetrics.partitionInfo().size(), dsFailInfo);
            for (final MetricsManager.PartitionInfo pMetrics : dsMetrics.partitionInfo()) { // Use MetricsManager type
                String pFailInfo = pMetrics.failureCause() != null ? "[FAIL: " + pMetrics.failureCause().getMessage() + "]" : "";
                System.out.printf("  Partition: %-20s | Status: %-4s | Duration: %5dms | Thread: %-25s | Batches: %d %s%n",
                        pMetrics.partitionId(), pMetrics.status(), pMetrics.duration().toMillis(), pMetrics.threadName(), pMetrics.batchMetrics().size(), pFailInfo);
                for (final MetricsManager.BatchInfo bMetrics : pMetrics.batchMetrics()) { // Use MetricsManager type
                    String procFail = bMetrics.failureCause() != null ? "[PROC_FAIL]" : "";
                    long loadFailCount = bMetrics.loadInfo().stream().filter(lm -> lm.status() == MetricsManager.Status.FAIL).count(); // Use MetricsManager type
                    String loadFail = loadFailCount > 0 ? "[LOAD_FAIL(" + loadFailCount + ")]" : "";
                    System.out.printf("    Batch: %-35s | Status: %-4s | Duration: %5dms | Thread: %-30s | Loads: %d %s %s%n",
                            bMetrics.batchName(), bMetrics.status(), bMetrics.duration().toMillis(), bMetrics.threadName(), bMetrics.loadInfo().size(), procFail, loadFail);
                }
            }
            System.out.println("  ----------------------------------------------------");
        }
        System.out.println("----------------------------------------------------------");
    }

    // --- Dummy Data Creation Helper ---
    private static void createDummyData(List<EtlPipelineItem> configs) { // Parameter is List<EtlPipelineItem>
        System.out.println("Creating dummy data directories/files for testing...");
        for (EtlPipelineItem conf : configs) { // Iterates over EtlPipelineItem
            SourceItem poll = conf.sources().getFirst(); //ToDo loop for multiple poll locations
            Path sourcePath = poll.sourceDir();
            try {
                Files.createDirectories(sourcePath);
                if (poll.useSubDirAsPartition()) {
                    for (int p = 1; p <= 3; p++) {
                        Path partPath = sourcePath.resolve("partition_" + p);
                        Files.createDirectories(partPath);
                        for (int f = 1; f <= 7; f++) {
                            String filter = poll.fileFilter();
                            String s = filter.contains("dat") ? ".dat" : filter.contains("txt") ? ".txt" : ".csv";
                            Path filePath = partPath.resolve("file_" + f + s);
                            if (!Files.exists(filePath)) Files.createFile(filePath);
                        }
                    }

                    if (poll.dirFilter() != null && !poll.dirFilter().equals("*"))
                        Files.createDirectories(sourcePath.resolve("ignored_partition"));

                } else {
                    for (int f = 1; f <= 5; f++) {
                        String ext = poll.fileFilter().contains("dat") ? ".dat" : (poll.fileFilter().contains("txt") ? ".txt" : ".csv");
                        Path filePath = sourcePath.resolve("base_file_" + f + ext);
                        if (!Files.exists(filePath)) Files.createFile(filePath);
                    }
                    if (!Files.exists(sourcePath.resolve("ignored_file.log")))
                        Files.createFile(sourcePath.resolve("ignored_file.log"));
                }
            } catch (IOException e) {
                System.err.println("Warning: Could not create dummy data for " + poll.sourceDir() + ": " + e.getMessage());
            }
        }
        System.out.println("Dummy data creation attempt finished.");
    }
}