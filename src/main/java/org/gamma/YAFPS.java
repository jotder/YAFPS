package org.gamma;

// Assume these classes exist in the com.gamma.config package
// and would be imported if FFPF.java is in a different package,
// or directly accessible if FFPF is also in com.gamma.config.
// For this example, we'll use fully qualified names or assume appropriate imports.

import org.gamma.config.AppConfig;
import org.gamma.config.ConfigManager;
import org.gamma.config.EtlPipelineItem;
// import org.gamma.config.SourceItem; // Unused
import org.gamma.metrics.MetricsManager;
import org.gamma.processing.DataSourceProcessor; // Added import

import java.io.IOException;
// import java.io.RandomAccessFile; // Unused
// import java.nio.channels.FileChannel; // Unused
// import java.nio.channels.FileLock; // Unused
// import java.nio.channels.OverlappingFileLockException; // Unused
// import java.nio.file.Path; // No longer used
// import java.nio.file.Files; // Unused
// import java.nio.file.PathMatcher; // Unused
// import java.nio.file.FileSystems; // Unused
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
// import java.util.Collections; // Unused
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// import java.util.concurrent.ForkJoinPool; // Unused
// import java.util.concurrent.ThreadFactory; // No longer used directly by YAFPS
// import java.util.concurrent.TimeUnit; // No longer used directly by YAFPS
// import java.util.concurrent.atomic.AtomicInteger; // Unused
// import java.util.Map; // Unused
// import java.util.LinkedHashMap; // Unused
// import java.util.concurrent.CompletionException; // No longer used directly by YAFPS
// import java.util.concurrent.CancellationException; // No longer used directly by YAFPS
import org.gamma.util.ConcurrencyUtils; // Added import


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
public class YAFPS {

    // --- Constants ---
    // SIMULATED_PROCESSING_FAILURE_MODULO moved to BatchProcessor.java
    // SIMULATED_LOAD_FAILURE_MODULO and SIMULATED_LOAD_DURATION moved to LoadSimulator.java
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
    public YAFPS(final AppConfig sourceConfigs) {
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
        final List<MetricsManager.DataSourceInfo> finalDataSourceMetrics;

        final int dataSourceConcurrency = Math.max(1, getSources().size());
        // Use ConcurrencyUtils for thread factory
        final java.util.concurrent.ThreadFactory dataSourceFactory = ConcurrencyUtils.createPlatformThreadFactory("DataSourceExec-");
        final ExecutorService dataSourceExecutor = Executors.newFixedThreadPool(dataSourceConcurrency, dataSourceFactory);
        final List<CompletableFuture<MetricsManager.DataSourceInfo>> dataSourceFutures = new ArrayList<>();

        System.out.printf("Starting execution with %d concurrent data sources.%n", dataSourceConcurrency);

        try {
            for (final EtlPipelineItem config : getSources()) { // Changed configItem back to config
                // Instantiate DataSourceProcessor for each config
                // Updated DataSourceProcessor instantiation, no longer passing 'this'
                final DataSourceProcessor dataSourceProcessor = new DataSourceProcessor(config); // Changed configItem back to config
                final CompletableFuture<MetricsManager.DataSourceInfo> dataSourceFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                // Call processDataSource on the DataSourceProcessor instance
                                return dataSourceProcessor.processDataSource();
                            } catch (final RuntimeException e) {
                                System.err.printf("!!! Uncaught RuntimeException processing data source %s: %s%n", config.pipelineName(), e.getMessage()); // Changed configItem back to config
                                e.printStackTrace(System.err);
                                // Create failed metrics, consider moving this utility to MetricsManager or a shared helper
                                return MetricsManager.createFailedDataSourceMetrics(config, e); // Changed configItem back to config
                            }
                        },
                        dataSourceExecutor
                );
                dataSourceFutures.add(dataSourceFuture);
            }

            // Use ConcurrencyUtils.waitForCompletableFuturesAndCollect
            // The existing ConcurrencyUtils.waitForCompletableFuturesAndCollect is generic <T>,
            // so it doesn't require <T extends MetricsManager.HasStatus>. This is fine.
            List<MetricsManager.DataSourceInfo> collectedResults = ConcurrencyUtils.waitForCompletableFuturesAndCollect(
                    "DataSource", dataSourceFutures, null);
            finalDataSourceMetrics = new CopyOnWriteArrayList<>(collectedResults);

        } finally {
            // Use ConcurrencyUtils for executor shutdown
            ConcurrencyUtils.shutdownExecutorService(dataSourceExecutor, "DataSourceExecutor");
        }

        System.out.println("All data source tasks completed or failed.");
        return new MetricsManager.ExecutionInfo(Duration.between(executionStart, Instant.now()), List.copyOf(finalDataSourceMetrics));
    }

    // --- Load Task Simulation --- (Moved to LoadSimulator.java)
    // public static MetricsManager.LoadInfo simulateLoad(final String fileName, final String targetTable) throws InterruptedException { ... }

    // determineOverallStatus has been moved to MetricsManager.java

    // --- Factory and Shutdown Methods --- (These are now removed as they are in ConcurrencyUtils)
    // public static ThreadFactory createPlatformThreadFactory(final String prefix) { ... }
    // public static void shutdownExecutorService(final ExecutorService executor, final String name) { ... }

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
        // Loop for pipelineName was just for illustration, removing
        // for (EtlPipelineItem pipelineItem : appConfig.etlPipelines()) {
        //     String pipelineName = pipelineItem.pipelineName();
        // }

        // createDummyData(appConfig.etlPipelines()); // Call moved to tests if needed

        final YAFPS executor = new YAFPS(appConfig); // Pass List<EtlPipelineItem>

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

    // --- Dummy Data Creation Helper --- (Moved to TestDataGenerator.java)
}