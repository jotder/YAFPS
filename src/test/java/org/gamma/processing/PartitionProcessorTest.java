package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.BatchInfo;
import org.gamma.metrics.PartitionInfo;
import org.gamma.metrics.Status;
import org.gamma.util.ConcurrencyUtils;
import org.gamma.util.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PartitionProcessorTest {

    @Mock
    private EtlPipelineItem mockConfig;
    @Mock
    private SourceItem mockSourceItem;

    private Path testPartitionPath = Paths.get("test_partition"); // Example path
    private String testPartitionId = "test_partition_1";

    // No YAFPSInstance mock needed as it was removed from PartitionProcessor's constructor previously

    @BeforeEach
    void setUp() {
        // Initialize mocks for every test
        lenient().when(mockConfig.pipelineName()).thenReturn("testPipeline");
        lenient().when(mockConfig.sources()).thenReturn(List.of(mockSourceItem));
        lenient().when(mockSourceItem.sourceId()).thenReturn("testSource");
        lenient().when(mockSourceItem.fileFilter()).thenReturn("*.txt");
        lenient().when(mockSourceItem.numThreads()).thenReturn(2); // Concurrency for batches
    }

    @AfterEach
    void tearDown() {
        // General cleanup if any, e.g., shutting down shared executors if they were used directly here.
        // In this test setup, executors are mostly managed within try-with-resources for MockedStatic or locally.
    }

    private BatchInfo createMockBatchInfo(String id, Status status) {
        return new BatchInfo(id, "Batch-" + id, status, Duration.ofMillis(20), "thread-batch",
                status == Status.FAIL ? new Throwable("Simulated batch fail for id " + id) : null,
                Collections.emptyList());
    }

    @Test
    void testProcessPartition_success() throws Exception {
        List<Path> filesInBatch1 = List.of(Paths.get("f1.txt"), Paths.get("f2.txt"));
        List<Path> filesInBatch2 = List.of(Paths.get("f3.txt"));
        List<List<Path>> fileBatches = List.of(filesInBatch1, filesInBatch2);

        BatchInfo batchInfo1 = createMockBatchInfo("" + 1, Status.PASS);
        BatchInfo batchInfo2 = createMockBatchInfo("" + 2, Status.PASS);

        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            mockedFileUtils.when(() -> FileUtils.getFileBatches(eq(testPartitionPath), eq(mockConfig)))
                    .thenReturn(fileBatches);

            // Mock the behavior of ConcurrencyUtils.waitForCompletableFuturesAndCollect
            // This is key to controlling the "outcome" of the batch processing stage from PartitionProcessor's view
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Batch"), anyList(), eq(testPartitionId)))
                    .thenAnswer(invocation -> {
                        // We expect two futures to be passed in a real scenario.
                        // Here, we directly return the results those futures would have produced.
                        return List.of(batchInfo1, batchInfo2);
                    });

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString())).thenReturn(Executors.defaultThreadFactory());
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString())).thenAnswer(inv -> null);


            PartitionProcessor processor = new PartitionProcessor(mockConfig, testPartitionId, testPartitionPath);
            PartitionInfo partitionInfo = processor.processPartition();

            assertEquals(Status.PASS, partitionInfo.status());
            assertEquals(testPartitionId, partitionInfo.partitionId());
            assertEquals(2, partitionInfo.batchMetrics().size());
            assertNull(partitionInfo.failureCause());

            mockedFileUtils.verify(() -> FileUtils.getFileBatches(testPartitionPath, mockConfig));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.createPlatformThreadFactory(contains("Batch-")));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), contains(testPartitionId + "-BatchExecutor")));
            // Verify waitForCompletableFuturesAndCollect was called for "Batch" level
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Batch"), anyList(), eq(testPartitionId)));
        }
    }

    @Test
    void testProcessPartition_fileBatchingFails() throws Exception {
        IOException ioException = new IOException("Failed to list files");
        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class)) {
            mockedFileUtils.when(() -> FileUtils.getFileBatches(eq(testPartitionPath), eq(mockConfig)))
                    .thenThrow(ioException);

            PartitionProcessor processor = new PartitionProcessor(mockConfig, testPartitionId, testPartitionPath);
            PartitionInfo partitionInfo = processor.processPartition();

            assertEquals(Status.FAIL, partitionInfo.status());
            assertNotNull(partitionInfo.failureCause());
            assertSame(ioException, partitionInfo.failureCause());
        }
    }

    @Test
    void testProcessPartition_noFilesFound() throws Exception {
        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)
        ) {

            mockedFileUtils.when(() -> FileUtils.getFileBatches(eq(testPartitionPath), eq(mockConfig)))
                    .thenReturn(Collections.emptyList()); // No files found

            PartitionProcessor processor = new PartitionProcessor(mockConfig, testPartitionId, testPartitionPath);
            PartitionInfo partitionInfo = processor.processPartition();

            assertEquals(Status.PASS, partitionInfo.status(), "Status should be PASS if no files are found (not an error).");
            assertTrue(partitionInfo.batchMetrics().isEmpty(), "Batch metrics should be empty.");
            assertNull(partitionInfo.failureCause());

            // Verify that batch processing utilities were not called if there were no batches
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.createPlatformThreadFactory(contains("Batch-")), times(0));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()), times(0));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Batch"), anyList(), anyString()), times(0));
        }
    }


    @Test
    void testProcessPartition_oneBatchFails() throws Exception {
        List<List<Path>> fileBatches = List.of(List.of(Paths.get("f1.txt"))); // One batch
        BatchInfo failingBatchInfo = createMockBatchInfo("" + 1, Status.FAIL);

        try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            mockedFileUtils.when(() -> FileUtils.getFileBatches(eq(testPartitionPath), eq(mockConfig)))
                    .thenReturn(fileBatches);

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("Batch"), anyList(), eq(testPartitionId)))
                    .thenReturn(List.of(failingBatchInfo)); // Simulate one failing batch result

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString())).thenReturn(Executors.defaultThreadFactory());
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString())).thenAnswer(inv -> null);


            PartitionProcessor processor = new PartitionProcessor(mockConfig, testPartitionId, testPartitionPath);
            PartitionInfo partitionInfo = processor.processPartition();

            assertEquals(Status.FAIL, partitionInfo.status());
            assertEquals(1, partitionInfo.batchMetrics().size());
            assertEquals(Status.FAIL, partitionInfo.batchMetrics().getFirst().status());
            assertNotNull(partitionInfo.batchMetrics().getFirst().failureCause(), "Failure cause for the batch should be set.");
        }
    }
}
