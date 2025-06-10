package org.gamma.ffpf;

import org.gamma.config.EtlPipelineItem;
import org.gamma.metrics.DataSourceInfo;
import org.gamma.metrics.ExecutionInfo;
import org.gamma.metrics.StatusHelper;
import org.gamma.processing.DataSourceHandler;
import org.gamma.util.ConcurrencyUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Orchestrates the processing of multiple data sources concurrently.
 */
public class FastFileExecutor {

    private final List<EtlPipelineItem> configs;


    /**
     * Constructor for the executor.
     *
     * @param configs List of configurations for the data sources to process.
     */
    public FastFileExecutor(final List<EtlPipelineItem> configs) {
        this.configs = Objects.requireNonNull(configs, "Data source configurations cannot be null");

        if (configs.isEmpty()) System.err.println("Warning: No source configurations provided.");
    }

    /**
     * Executes the processing for all configured data sources.
     *
     * @return ExecutionInfo containing results and timings.
     */
    public ExecutionInfo execute() throws IOException {

        final Path sourceDirPath = Paths.get(".");
        final Path lockFilePath = sourceDirPath.resolve(".executor.lock");

        try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel();
             FileLock lock = channel.tryLock()) {
            if (lock == null) {
                System.err.printf("!!!! WARN: Could not acquire lock (%s), another instance already running ??? %n", lockFilePath);
                throw new RuntimeException("Skipped due to existing lock file: " + lockFilePath);
            }
        }

        final Instant executionStart = Instant.now();
        final List<DataSourceInfo> metrics;

        final int maxInstance = Math.max(1, configs.size());
        final ThreadFactory factory = ConcurrencyUtils.createPlatformThreadFactory("DataSource-");
        final ExecutorService execService = Executors.newFixedThreadPool(maxInstance, factory);

        final List<CompletableFuture<DataSourceInfo>> futures = new ArrayList<>();

        System.out.printf("%nStarting execution with %d concurrent data sources.%n", maxInstance);

        try {
            for (EtlPipelineItem conf : configs) {

                final DataSourceHandler processor = new DataSourceHandler(conf);
                final CompletableFuture<DataSourceInfo> future = CompletableFuture.supplyAsync(() -> {
                            try {
                                return processor.process();
                            } catch (final RuntimeException e) {
                                System.err.printf("!!! Exception processing data source %s: %s%n",
                                        conf.pipelineName(), e.getMessage());
                                e.printStackTrace(System.err);
                                return StatusHelper.createFailedDataSourceInfo(conf, e);
                            } catch (IOException e) {
                                e.printStackTrace();
                                System.err.printf("!!! Data source %s: %s directory doesn't exists%n",
                                        conf.pipelineName(), e.getMessage());
                                return StatusHelper.createFailedDataSourceInfo(conf, e);
                            }
                        },
                        execService
                );
                futures.add(future);
            }

            metrics = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("DataSource", futures, null));

        } finally {
            ConcurrencyUtils.shutdownExecutorService(execService, "DataSourceExecutor");
        }

        System.out.println("All data source tasks completed or failed.");
        return new ExecutionInfo(Duration.between(executionStart, Instant.now()), List.copyOf(metrics));
    }

}
    