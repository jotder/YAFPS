package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit; // For awaitTermination

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataSourceProcessorTest {

    @Mock
    private EtlPipelineItem mockConfig;
    @Mock
    private SourceItem mockSourceItem;

    @TempDir
    Path tempDir;

    private Path sourceDirPath;
    private Path lockFilePath;
    // No YAFPSInstance mock needed as it was removed from DataSourceProcessor's constructor

    @BeforeEach
    void setUp() throws IOException {
        sourceDirPath = tempDir.resolve("source_dir_" + System.nanoTime()); // Unique source dir per test
        Files.createDirectories(sourceDirPath);
        lockFilePath = sourceDirPath.resolve(".fast_executor.lock");

        lenient().when(mockConfig.pipelineName()).thenReturn("testDsPipeline");
        lenient().when(mockConfig.sources()).thenReturn(List.of(mockSourceItem));
        lenient().when(mockSourceItem.sourceDir()).thenReturn(sourceDirPath);
        lenient().when(mockSourceItem.sourceId()).thenReturn("testSourceId");
        lenient().when(mockSourceItem.batchSize()).thenReturn(1); // Partition concurrency
        lenient().when(mockSourceItem.useSubDirAsPartition()).thenReturn(true); // Default to using subdirs
        lenient().when(mockSourceItem.dirFilter()).thenReturn(null); // Default to no filter
    }

    @AfterEach
    void tearDown() throws IOException {
        // Attempt to clean up lock file if created.
        // This is important as @TempDir might not clean it if a FileChannel is open or if test fails abruptly.
        try {
            Files.deleteIfExists(lockFilePath);
        } catch (IOException e) {
            // System.err.println("Could not delete lock file in tearDown: " + lockFilePath + " - " + e.getMessage());
        }
    }

    private MetricsManager.PartitionInfo createMockPartitionInfo(String id, MetricsManager.Status status) {
        return new MetricsManager.PartitionInfo("testSourceId", id, status, Duration.ofMillis(50), "thread-part",
                Collections.emptyList(), // Assuming no batch metrics needed for this level of test
                status == MetricsManager.Status.FAIL ? new Throwable("Simulated partition fail for id " + id) : null);
    }

    @Test
    void testProcessDataSource_success() throws Exception {
        Path part1Path = sourceDirPath.resolve("part1"); Files.createDirectory(part1Path);
        Path part2Path = sourceDirPath.resolve("part2"); Files.createDirectory(part2Path);

        MetricsManager.PartitionInfo partInfo1 = createMockPartitionInfo("part1_1", MetricsManager.Status.PASS);
        MetricsManager.PartitionInfo partInfo2 = createMockPartitionInfo("part2_2", MetricsManager.Status.PASS);

        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class);
             MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class)) {

            mockedFileUtils.when(() -> FileUtils.getDirectoriesAsPartition(eq(sourceDirPath), any()))
                           .thenReturn(List.of(part1Path, part2Path));

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Partition"), anyList(), eq("testSourceId")))
                               .thenAnswer(invocation -> List.of(partInfo1, partInfo2)); // Return resolved infos

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString())).thenReturn(Executors.defaultThreadFactory());
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString())).thenAnswer(inv -> null);

            DataSourceProcessor processor = new DataSourceProcessor(mockConfig);
            MetricsManager.DataSourceInfo dsInfo = processor.processDataSource();

            assertEquals(MetricsManager.Status.PASS, dsInfo.status());
            assertEquals("testDsPipeline", dsInfo.sourceName());
            assertEquals(2, dsInfo.partitionInfo().size());
            assertNull(dsInfo.failureCause());
            assertTrue(Files.exists(lockFilePath), "Lock file should have been created and then released (still exists if not deleted by finally).");
        }
    }

    @Test
    void testProcessDataSource_lockFileFails_OverlappingLock() throws Exception {
        // Pre-create and lock the file in the test's sourceDirPath
        try (FileChannel channel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock fileLock = channel.tryLock()) { // Keep the lock

            assertNotNull(fileLock, "Test setup: Failed to acquire lock for pre-locking.");
            assertTrue(fileLock.isValid());

            DataSourceProcessor processor = new DataSourceProcessor(mockConfig);
            MetricsManager.DataSourceInfo dsInfo = processor.processDataSource();

            assertEquals(MetricsManager.Status.PASS, dsInfo.status(), "Status should be PASS (skip) if lock cannot be acquired.");
            assertNotNull(dsInfo.failureCause(), "Failure cause should be set for overlapping lock.");
            assertTrue(dsInfo.failureCause().getMessage().contains("Skipped due to overlapping lock"), "Failure cause message mismatch. Actual: " + dsInfo.failureCause().getMessage());
            assertTrue(dsInfo.partitionInfo().isEmpty());
        } // fileLock and channel are released here
    }


    @Test
    void testProcessDataSource_discoverPartitionsFails() throws Exception {
        IOException ioException = new IOException("Failed to list directories");
        when(mockSourceItem.useSubDirAsPartition()).thenReturn(true);

        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class)) {
             mockedFileUtils.when(() -> FileUtils.getDirectoriesAsPartition(eq(sourceDirPath), any()))
                           .thenThrow(ioException);

            DataSourceProcessor processor = new DataSourceProcessor(mockConfig);
            MetricsManager.DataSourceInfo dsInfo = processor.processDataSource();

            assertEquals(MetricsManager.Status.FAIL, dsInfo.status());
            assertNotNull(dsInfo.failureCause());
            // The ioException from discoverPartitions is wrapped in a RuntimeException by processDataSource's catch-all
            // then that RuntimeException is passed to createFailedDataSourceMetrics.
            // The original design had createFailedDataSourceMetrics take the direct exception.
            // Let's check the cause of the failureCause.
            assertTrue(dsInfo.failureCause().getMessage().contains("Failed to list/discover partitions"));
            assertSame(ioException, dsInfo.failureCause().getCause(), "The cause of failure should be the IO Exception from discoverPartitions.");
        }
    }

    @Test
    void testProcessDataSource_noPartitionsFound() throws Exception {
        when(mockSourceItem.useSubDirAsPartition()).thenReturn(true);
        // dirFilter is already null by default from setUp, or can be more specific
        // when(mockSourceItem.dirFilter()).thenReturn("glob:non_matching_*");


        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            mockedFileUtils.when(() -> FileUtils.getDirectoriesAsPartition(eq(sourceDirPath), any()))
                           .thenReturn(Collections.emptyList());

            DataSourceProcessor processor = new DataSourceProcessor(mockConfig);
            MetricsManager.DataSourceInfo dsInfo = processor.processDataSource();

            assertEquals(MetricsManager.Status.PASS, dsInfo.status(), "Status should be PASS if no partitions are found.");
            assertTrue(dsInfo.partitionInfo().isEmpty());
            assertNull(dsInfo.failureCause());

            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString()), times(0));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()), times(0));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Partition"), anyList(), any()), times(0));
        }
    }

    @Test
    void testProcessDataSource_onePartitionFails() throws Exception {
        Path part1Path = sourceDirPath.resolve("part1"); Files.createDirectory(part1Path);
        when(mockSourceItem.useSubDirAsPartition()).thenReturn(true);

        MetricsManager.PartitionInfo failingPartInfo = createMockPartitionInfo("part1_1", MetricsManager.Status.FAIL);

        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class);
             MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class)) {

            mockedFileUtils.when(() -> FileUtils.getDirectoriesAsPartition(eq(sourceDirPath), any()))
                           .thenReturn(List.of(part1Path));

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Partition"), anyList(), eq("testSourceId")))
                               .thenReturn(List.of(failingPartInfo));

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString())).thenReturn(Executors.defaultThreadFactory());
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString())).thenAnswer(inv -> null);

            DataSourceProcessor processor = new DataSourceProcessor(mockConfig);
            MetricsManager.DataSourceInfo dsInfo = processor.processDataSource();

            assertEquals(MetricsManager.Status.FAIL, dsInfo.status());
            assertEquals(1, dsInfo.partitionInfo().size());
            assertEquals(MetricsManager.Status.FAIL, dsInfo.partitionInfo().getFirst().status());
            assertNotNull(dsInfo.partitionInfo().getFirst().failureCause());
        }
    }
}
