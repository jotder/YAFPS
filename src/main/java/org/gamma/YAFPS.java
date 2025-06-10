package org.gamma;

import org.gamma.config.AppConfig;
import org.gamma.config.ConfigManager;
import org.gamma.config.EtlPipelineItem;
import org.gamma.metrics.*;
import org.gamma.processing.DataSourceProcessor;
import org.gamma.util.ConcurrencyUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;


/**
 * Processes data sources, partitions, batches, and loads files concurrently
 * using CompletableFuture and ExecutorService.
 * Uses Virtual Threads for the final loading phase within each batch.
 * Configuration is expected to be provided as a List of EtlPipelineItem objects,
 * typically loaded by a ConfigManager from a YAML file.
 */
public class YAFPS {

    private final AppConfig appConfig; // Uses the new EtlPipelineItem

    /**
     * Constructor for the executor.
     *
     * @param appConfig List of configurations for the data sources to process.
     *                  These are instances of {@link AppConfig}, typically loaded
     *                  by a {@code com.gamma.config.ConfigManager}.
     */
    public YAFPS(final AppConfig appConfig) {
        this.appConfig = Objects.requireNonNull(appConfig, "Data source configurations cannot be null");
        if (appConfig.etlPipelines().isEmpty()) {
            System.err.println("Warning: No source configurations provided.");
        }
    }

    // --- Main Method ---
    public static void main(final String[] args) throws IOException {

        Path lockFilePath = Path.of(".").resolve(".executor.lock");
        try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel();
             FileLock lock = channel.tryLock()) {
            if (lock == null) {
                System.err.printf("!!!! WARN: Could not acquire lock (%s), another instance already running ??? %n", lockFilePath);
                throw new RuntimeException("Skipped due to existing lock file: " + lockFilePath);
            }
        }

        AppConfig appConfig = ConfigManager.getConfig();

        final YAFPS executor = new YAFPS(appConfig); // Pass List<EtlPipelineItem>

        try {
            System.out.println("========================================================");
            System.out.println(" Starting Execution (CompletableFuture with Externalized Config Model) ");
            System.out.println("========================================================");

            ExecutionInfo metrics = executor.execute(); // Use MetricsManager type

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
    private static void printMetricsSummary(final ExecutionInfo metrics) { // Use MetricsManager type
        System.out.println("Total Execution Time: " + metrics.totalDuration().toMillis() + " ms");
        System.out.println("---------------------- METRICS SUMMARY ----------------------");
        for (final DataSourceInfo dsMetrics : metrics.dataSourceInfo()) { // Use MetricsManager type
            String dsFailInfo = dsMetrics.failureCause() != null ? "[FAIL: " + dsMetrics.failureCause().getMessage() + "]" : "";
            System.out.printf("Data Source: %-10s | Status: %-4s | Duration: %5dms | Thread: %-20s | Partitions: %d %s%n",
                    dsMetrics.sourceName(), dsMetrics.status(), dsMetrics.duration().toMillis(), dsMetrics.threadName(), dsMetrics.partitionInfo().size(), dsFailInfo);
            for (final PartitionInfo pMetrics : dsMetrics.partitionInfo()) { // Use MetricsManager type
                String pFailInfo = pMetrics.failureCause() != null ? "[FAIL: " + pMetrics.failureCause().getMessage() + "]" : "";
                System.out.printf("  Partition: %-20s | Status: %-4s | Duration: %5dms | Thread: %-25s | Batches: %d %s%n",
                        pMetrics.partitionId(), pMetrics.status(), pMetrics.duration().toMillis(), pMetrics.threadName(), pMetrics.batchMetrics().size(), pFailInfo);
                for (final BatchInfo bMetrics : pMetrics.batchMetrics()) { // Use MetricsManager type
                    String procFail = bMetrics.failureCause() != null ? "[PROC_FAIL]" : "";
                    long loadFailCount = bMetrics.loadingInfo().stream().filter(lm -> lm.status() == Status.FAIL).count(); // Use MetricsManager type
                    String loadFail = loadFailCount > 0 ? "[LOAD_FAIL(" + loadFailCount + ")]" : "";
                    System.out.printf("    Batch: %-35s | Status: %-4s | Duration: %5dms | Thread: %-30s | Loads: %d %s %s%n",
                            bMetrics.batchName(), bMetrics.status(), bMetrics.duration().toMillis(), bMetrics.threadName(), bMetrics.loadingInfo().size(), procFail, loadFail);
                }
            }
            System.out.println("  ----------------------------------------------------");
        }
        System.out.println("----------------------------------------------------------");
    }

    /**
     * Gets the list of source configurations.
     *
     * @return The list of {@link EtlPipelineItem} objects.
     */
    List<EtlPipelineItem> getSources() {
        return appConfig.etlPipelines();
    }

    // --- Entry Point ---
    public ExecutionInfo execute() throws InterruptedException { // Use MetricsManager type
        final Instant executionStart = Instant.now();
        final List<DataSourceInfo> finalDataSourceMetrics;

        final int dataSourceConcurrency = Math.max(1, getSources().size());

        final ThreadFactory dataSourceFactory = ConcurrencyUtils.createPlatformThreadFactory("DataSourceExec-");
        final ExecutorService dataSourceExecutor = Executors.newFixedThreadPool(dataSourceConcurrency, dataSourceFactory);
        final List<CompletableFuture<DataSourceInfo>> dataSourceFutures = new ArrayList<>();

        System.out.printf("Starting execution with %d concurrent data sources.%n", dataSourceConcurrency);

        try {
            for (final EtlPipelineItem config : getSources()) {
                final DataSourceProcessor processor = new DataSourceProcessor(config);
                final CompletableFuture<DataSourceInfo> dataSourceFuture = CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return processor.processDataSource();   // Call processDataSource on the DataSourceProcessor instance
                            } catch (final RuntimeException e) {
                                System.err.printf("!!! Uncaught RuntimeException processing data source %s: %s%n", config.pipelineName(), e.getMessage());
                                e.printStackTrace(System.err);
                                // Create failed metrics, consider moving this utility to MetricsManager or a shared helper
                                return StatusHelper.createFailedDataSourceInfo(config, e);
                            }
                        },
                        dataSourceExecutor
                );
                dataSourceFutures.add(dataSourceFuture);
            }

            List<DataSourceInfo> collectedResults = ConcurrencyUtils.waitForCompletableFuturesAndCollect(
                    "DataSource", dataSourceFutures, null);
            finalDataSourceMetrics = new CopyOnWriteArrayList<>(collectedResults);

        } finally {
            ConcurrencyUtils.shutdownExecutorService(dataSourceExecutor, "DataSourceExecutor");
        }

        System.out.println("All data source tasks completed or failed.");
        return new ExecutionInfo(Duration.between(executionStart, Instant.now()), List.copyOf(finalDataSourceMetrics));
    }
}