package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.*;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataSourceProcessor {

    private final EtlPipelineItem config;

    public DataSourceProcessor(EtlPipelineItem config) {
        this.config = config;
    }

    public DataSourceInfo processDataSource() {
        final Instant sourceStart = Instant.now();
        final String sourceName = "%s".formatted(config.pipelineName());
        final String currentThreadName = Thread.currentThread().getName();
        System.out.printf("%nProcessing Data Source : %s on Thread %s%n", sourceName, currentThreadName);
        SourceItem pollInf = config.sources().getFirst();
        List<Path> partitionsToProcess;
        try {
            partitionsToProcess = this.discoverPartitions(config, sourceName);
        } catch (final IOException e) {
            System.err.printf("!!! FATAL: Failed to list/discover partitions for Source %s: %s%n", sourceName, e.getMessage());
            return StatusHelper.createFailedDataSourceInfo(config, e);
        }

        if (partitionsToProcess.isEmpty() && pollInf.useSubDirAsPartition()) {
            System.out.printf("  Source %s: No matching partition directories found. Skipping.%n", sourceName);
            return new DataSourceInfo(config.pipelineName(), sourceName, Status.PASS, Duration.ZERO, currentThreadName, List.of());
        }

        System.out.printf("  Source %s: Submitting %d partitions (partitionConcurrency=%d).%n", sourceName, partitionsToProcess.size(), pollInf.batchSize());

        List<PartitionInfo> partitionResults;
        Status sourceStatus;
        final int partitionConcurrency = Math.max(1, pollInf.batchSize());
        final ThreadFactory partitionFactory = ConcurrencyUtils.createPlatformThreadFactory(sourceName + "-Partition-");
        final ExecutorService partitionExecutor = Executors.newFixedThreadPool(partitionConcurrency, partitionFactory);
        final List<CompletableFuture<PartitionInfo>> partitionFutures = new ArrayList<>();
        final AtomicInteger partitionCounter = new AtomicInteger(1);

        try {
            for (final Path partitionPath : partitionsToProcess) {
                final String partitionId = partitionPath.getFileName() + "_" + partitionCounter.getAndIncrement();
                partitionFutures.add(CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                PartitionProcessor partitionProcessor = new PartitionProcessor(this.config, partitionId, partitionPath);
                                return partitionProcessor.processPartition();
                            } catch (final RuntimeException e) {
                                System.err.printf("!!! Uncaught RuntimeException processing partition %s for source %s: %s%n", partitionId, sourceName, e.getMessage());
                                e.printStackTrace(System.err);
                                return StatusHelper.createFailedPartitionInfo(this.config.sources().getFirst().sourceId(), partitionId, e);
                            }
                        }, partitionExecutor));
            }
            partitionResults = new CopyOnWriteArrayList<>(ConcurrencyUtils.waitForCompletableFuturesAndCollect("Partition", partitionFutures, pollInf.sourceId()));
            sourceStatus = StatusHelper.determineOverallStatus(partitionResults, partitionsToProcess.size(), "Source", sourceName);
        } finally {
            ConcurrencyUtils.shutdownExecutorService(partitionExecutor, sourceName + "-PartitionExecutor");
        }
        System.out.printf("Finished Data Source %s%n", sourceName);
        return new DataSourceInfo(pollInf.sourceId(), sourceName, sourceStatus, Duration.between(sourceStart, Instant.now()), currentThreadName, List.copyOf(partitionResults));

    }

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
            List<Path> foundPartitions = FileUtils.getDirectoriesAsPartition(pollInf.sourceDir(), pollInf.dirFilter());
            System.out.printf("  Source %s: Found %d partitions matching '%s'.%n", sourceName, foundPartitions.size(), pollInf.dirFilter());
            return foundPartitions;
        } else {
            System.out.printf("  Source %s: Processing '%s' as single partition.%n", sourceName, pollInf.sourceDir());
            return List.of(sourceDirPath);
        }
    }
}
