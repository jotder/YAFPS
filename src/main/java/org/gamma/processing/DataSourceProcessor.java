package org.gamma.processing;

import org.gamma.YAFPS; // YAFPF instance might still be needed for non-metrics/non-config methods
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.FileUtils; // Added import
import static org.gamma.metrics.MetricsManager.*; // Import all static members, including Status, DataSourceInfo, PartitionInfo etc.

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files; // Added import
import java.nio.file.FileSystems; // Added import
import java.nio.file.PathMatcher; // Added import
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

    // private final YAFPS YAFPSInstance; // No longer needed as PartitionProcessor doesn't require it
    private final EtlPipelineItem config;

    // Updated constructor, removed YAFPSInstance
    public DataSourceProcessor(EtlPipelineItem config) {
        // this.YAFPSInstance = YAFPSInstance;
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
                partitionsToProcess = this.discoverPartitions(config, sourceName); // Changed to local call
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
            final ThreadFactory partitionFactory = ConcurrencyUtils.createPlatformThreadFactory(sourceName + "-Partition-"); // Changed to ConcurrencyUtils
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
                                    // Updated PartitionProcessor instantiation
                                    PartitionProcessor partitionProcessor = new PartitionProcessor(this.config, partitionId, partitionPath);
                                    return partitionProcessor.processPartition(); // This will return MetricsManager.PartitionInfo
                                } catch (final RuntimeException e) {
                                    System.err.printf("!!! Uncaught RuntimeException processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                    e.printStackTrace(System.err);
                                    return MetricsManager.createFailedPartitionMetrics(this.config.sources().getFirst().sourceId(), partitionId, e); // Use MetricsManager method
                                }
                            }, partitionExecutor));
                }
                partitionResults = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Partition", partitionFutures, pollInf.sourceId())); // Changed to ConcurrencyUtils
                sourceStatus = MetricsManager.determineOverallStatus(partitionResults, partitionsToProcess.size(), "Source", sourceName); // Use MetricsManager method
            } finally {
                ConcurrencyUtils.shutdownExecutorService(partitionExecutor, sourceName + "-PartitionExecutor"); // Changed to ConcurrencyUtils
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

    // Copied from YAFPS.java, made non-static and private
    // Temporarily calling YAFPS.getDirectoriesAsPartition, will be FileUtils later
    private List<Path> discoverPartitions(EtlPipelineItem conf, String sourceName) throws IOException {
        SourceItem pollInf = conf.sources().getFirst();
        Path sourceDirPath = pollInf.sourceDir();
        if (!Files.isDirectory(sourceDirPath)) {
            throw new IOException("Source directory does not exist: " + sourceDirPath);
        }

        if (pollInf.useSubDirAsPartition()) {
            // TODO: Replace with FileUtils.getDirectoriesAsPartition once available
            // For now, assuming YAFPS still has a static getDirectoriesAsPartition or it's a placeholder.
            // This will likely cause a compile error if YAFPS.getDirectoriesAsPartition was removed and not yet in FileUtils.
            // List<Path> foundPartitions = YAFPS.getDirectoriesAsPartition(pollInf.sourceDir(), pollInf.dirFilter());
            // Placeholder call, assuming getDirectoriesAsPartition will be in FileUtils
             List<Path> foundPartitions = FileUtils.getDirectoriesAsPartition(pollInf.sourceDir(), pollInf.dirFilter()); // Now uses import
            System.out.printf("  Source %s: Found %d partitions matching '%s'.%n", sourceName, foundPartitions.size(), pollInf.dirFilter());
            return foundPartitions;
        } else {
            System.out.printf("  Source %s: Processing '%s' as single partition.%n", sourceName, pollInf.sourceDir());
            return List.of(sourceDirPath);
        }
    }
}
