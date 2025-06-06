package org.gamma.processing;

import org.gamma.YAFPF; // YAFPF instance might still be needed for non-metrics/non-config methods
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import static org.gamma.metrics.MetricsManager.*; // Import all static members, including Status, DataSourceInfo, PartitionInfo etc.

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
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

public class DataSourceProcessor {

    private final YAFPF yafpfInstance; // To call non-static helpers from YAFPF
    private final EtlPipelineItem config;

    public DataSourceProcessor(YAFPF yafpfInstance, EtlPipelineItem config) {
        this.yafpfInstance = yafpfInstance;
        this.config = config;
    }

    public DataSourceInfo processDataSource() { // Use MetricsManager.DataSourceInfo (via static import)
        final Instant sourceStart = Instant.now();
        final String sourceName = "%s".formatted(config.pipelineName()); // use this.config
        final String currentThreadName = Thread.currentThread().getName();
        System.out.printf("%nProcessing Data Source : %s on Thread %s%n", sourceName, currentThreadName);
        SourceItem pollInf = config.sources().getFirst(); // use this.config
        final Path sourceDirPath = pollInf.sourceDir();
        final Path lockFilePath = sourceDirPath.resolve(".fast_executor.lock");

        try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel();
             FileLock lock = channel.tryLock()) {

            if (lock == null) {
                System.err.printf("!!! WARN: Could not acquire lock for Source %s (%s). Already processed? Skipping.%n", sourceName, lockFilePath);
                return new DataSourceInfo(config.pipelineName(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of(), // Use MetricsManager types
                        new RuntimeException("Skipped due to existing lock file: " + lockFilePath));
            }
            System.out.printf("  Source %s: Acquired lock file %s%n", sourceName, lockFilePath);

            List<Path> partitionsToProcess;
            try {
                partitionsToProcess = YAFPF.discoverPartitions(config, sourceName); // This YAFPF method is not metrics related
            } catch (final IOException e) {
                System.err.printf("!!! FATAL: Failed to list/discover partitions for Source %s: %s%n", sourceName, e.getMessage());
                return MetricsManager.createFailedDataSourceMetrics(config, e); // Use MetricsManager method
            }

            if (partitionsToProcess.isEmpty() && pollInf.useSubDirAsPartition()) {
                System.out.printf("  Source %s: No matching partition directories found. Skipping.%n", sourceName);
                return new DataSourceInfo(config.pipelineName(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of()); // Use MetricsManager types
            }

            System.out.printf("  Source %s: Submitting %d partitions (partitionConcurrency=%d).%n", sourceName, partitionsToProcess.size(), pollInf.batchSize());

            List<PartitionInfo> partitionResults; // Use MetricsManager.PartitionInfo (via static import)
            Status sourceStatus; // Use MetricsManager.Status (via static import)
            final int partitionConcurrency = Math.max(1, pollInf.batchSize());
            final ThreadFactory partitionFactory = YAFPF.createPlatformThreadFactory(sourceName + "-Partition-"); // This YAFPF method is not metrics related
            final ExecutorService partitionExecutor = Executors.newFixedThreadPool(partitionConcurrency, partitionFactory);
            final List<CompletableFuture<PartitionInfo>> partitionFutures = new ArrayList<>(); // Use MetricsManager.PartitionInfo
            final AtomicInteger partitionCounter = new AtomicInteger(1);

            try {
                for (final Path partitionPath : partitionsToProcess) {
                    final String partitionId = partitionPath.getFileName() + "_" + partitionCounter.getAndIncrement();
                    partitionFutures.add(CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    // Instantiate PartitionProcessor and call its processPartition method
                                    PartitionProcessor partitionProcessor = new PartitionProcessor(this.yafpfInstance, this.config, partitionId, partitionPath);
                                    return partitionProcessor.processPartition(); // This will return MetricsManager.PartitionInfo
                                } catch (final RuntimeException e) {
                                    System.err.printf("!!! Uncaught RuntimeException processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                    e.printStackTrace(System.err);
                                    return MetricsManager.createFailedPartitionMetrics(this.config.sources().getFirst().sourceId(), partitionId, e); // Use MetricsManager method
                                }
                            }, partitionExecutor));
                }
                partitionResults = new CopyOnWriteArrayList<>(YAFPF.waitForCompletableFuturesAndCollect("Partition", partitionFutures, pollInf.sourceId())); // This YAFPF method is generic
                sourceStatus = MetricsManager.determineOverallStatus(partitionResults, partitionsToProcess.size(), "Source", sourceName); // Use MetricsManager method
            } finally {
                YAFPF.shutdownExecutorService(partitionExecutor, sourceName + "-PartitionExecutor"); // This YAFPF method is not metrics related
            }
            System.out.printf("Finished Data Source %s%n", sourceName);
            return new DataSourceInfo(pollInf.sourceId(), sourceName, sourceStatus, Duration.between(sourceStart, Instant.now()), currentThreadName, List.copyOf(partitionResults)); // Use MetricsManager type
        } catch (OverlappingFileLockException e) {
            System.err.printf("!!! WARN: Lock for Source %s (%s) held by another process. Skipping. %s%n", sourceName, lockFilePath, e.getMessage());
            return new DataSourceInfo(pollInf.sourceId(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of(), // Use MetricsManager types
                    new RuntimeException("Skipped due to overlapping lock: " + lockFilePath, e));
        } catch (IOException e) {
            System.err.printf("!!! FATAL: Failed to access lock file for Source %s (%s): %s%n", sourceName, lockFilePath, e.getMessage());
            return MetricsManager.createFailedDataSourceMetrics(config, new IOException("Failed to acquire lock file: " + lockFilePath, e)); // Use MetricsManager method
        } finally {
            System.out.printf("  Source %s: Released lock file %s%n", sourceName, lockFilePath); // This is fine
        }
    }
}
