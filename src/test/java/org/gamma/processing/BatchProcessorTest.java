package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.BatchInfo;
import org.gamma.metrics.LoadingInfo;
import org.gamma.metrics.Status;
import org.gamma.util.ConcurrencyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class) // Initialize mocks
class BatchProcessorTest {

    @Mock
    private EtlPipelineItem mockConfig;
    @Mock
    private SourceItem mockSourceItem; // Although not directly used in these tests, good for consistency if config needs it

    private ExecutorService batchExecutor; // Real executor for the BatchProcessor

    @BeforeEach
    void setUp() {
        // Using a single-threaded executor for test predictability
        batchExecutor = Executors.newSingleThreadExecutor();
        // Common stubbing, even if not every test uses all parts of mockConfig directly
        // It's good practice if BatchProcessor internally relies on these.
        // For the current BatchProcessor, `config` is not used, but this is defensive.
        lenient().when(mockConfig.sources()).thenReturn(List.of(mockSourceItem));
        lenient().when(mockSourceItem.sourceId()).thenReturn("testSource");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (batchExecutor != null) {
            batchExecutor.shutdown();
            if (!batchExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                batchExecutor.shutdownNow();
            }
        }
    }

    // Test successful processing and loading
    @Test
    void testProcessBatch_success() throws Exception {
        List<Path> batchData = List.of(Paths.get("file1.txt"), Paths.get("file2.txt"));
        String batchId = "1"; // Non-failing processing ID

        LoadingInfo successfulLoad1 = new LoadingInfo("file1.txt", "table-x", Status.PASS, Duration.ofMillis(10), "thread-load1", null);
        LoadingInfo successfulLoad2 = new LoadingInfo("file2.txt", "table-y", Status.PASS, Duration.ofMillis(10), "thread-load2", null);

        try (MockedStatic<LoadSimulator> mockedLoadSimulator = mockStatic(LoadSimulator.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            // IMPORTANT: Matcher for table name needs to be flexible if it's dynamically generated
            mockedLoadSimulator.when(() -> LoadSimulator.simulateLoad(eq("file1.txt"), anyString()))
                    .thenReturn(successfulLoad1);
            mockedLoadSimulator.when(() -> LoadSimulator.simulateLoad(eq("file2.txt"), anyString()))
                    .thenReturn(successfulLoad2);

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(invocation -> null); // Just do nothing

            BatchProcessor processor = new BatchProcessor(mockConfig, batchId, batchData, batchExecutor);
            CompletableFuture<BatchInfo> futureBatchInfo = processor.processBatch();
            BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS); // Wait for completion with timeout

            assertEquals(Status.PASS, batchInfo.status(), "Batch status should be PASS.");
            assertEquals(batchId, batchInfo.microBatchId());
            assertNotNull(batchInfo.batchName());
            assertEquals(2, batchInfo.loadingInfo().size(), "Should have load info for 2 files.");
            assertNull(batchInfo.failureCause(), "Failure cause should be null for successful batch.");

            // Verify simulateLoad was called with the correct file names and any string for table name
            mockedLoadSimulator.verify(() -> LoadSimulator.simulateLoad(eq("file1.txt"), anyString()), times(1));
            mockedLoadSimulator.verify(() -> LoadSimulator.simulateLoad(eq("file2.txt"), anyString()), times(1));
        }
    }

    // Test processing failure (SIMULATED_PROCESSING_FAILURE_MODULO)
    @Test
    void testProcessBatch_processingFailure() throws Exception {
        String failingBatchId = BatchProcessor.SIMULATED_PROCESSING_FAILURE_MODULO;
        List<Path> batchData = List.of(Paths.get("file_proc_fail.txt"));

        // No need to mock LoadSimulator as it shouldn't be called if processing fails first.
        // ConcurrencyUtils for shutting down the internal loadExecutor might still be called in a finally block.
        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(invocation -> null);

            BatchProcessor processor = new BatchProcessor(mockConfig, failingBatchId, batchData, batchExecutor);
            CompletableFuture<BatchInfo> futureBatchInfo = processor.processBatch();
            BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(Status.FAIL, batchInfo.status(), "Batch status should be FAIL due to processing error.");
            assertNotNull(batchInfo.failureCause(), "Failure cause should be set for processing failure.");
            // Check the root cause if it's wrapped by CompletionException from the supplyAsync
            Throwable rootCause = batchInfo.failureCause();
            if (rootCause instanceof java.util.concurrent.CompletionException && rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertTrue(rootCause.getMessage().contains("Simulated processing failure"), "Cause should indicate simulated processing failure.");
            assertTrue(batchInfo.loadingInfo().isEmpty(), "Load info should be empty on processing failure.");
        }
    }

    // Test load failure (one file fails to load)
    @Test
    void testProcessBatch_loadFailure_oneFile() throws Exception {
        List<Path> batchData = List.of(Paths.get("file_load_ok.txt"), Paths.get("file_load_fail.txt"));
        String batchId = "1"; // Non-failing processing ID

        LoadingInfo successfulLoad = new LoadingInfo("file_load_ok.txt", "table-1", Status.PASS, Duration.ofMillis(10), "thread-load1", null);
        RuntimeException simulatedLoadException = new RuntimeException("Simulated DB error for file_load_fail.txt");

        try (MockedStatic<LoadSimulator> mockedLoadSimulator = mockStatic(LoadSimulator.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            mockedLoadSimulator.when(() -> LoadSimulator.simulateLoad(eq("file_load_ok.txt"), anyString()))
                    .thenReturn(successfulLoad);
            mockedLoadSimulator.when(() -> LoadSimulator.simulateLoad(eq("file_load_fail.txt"), anyString()))
                    .thenThrow(simulatedLoadException);

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(invocation -> null);

            BatchProcessor processor = new BatchProcessor(mockConfig, batchId, batchData, batchExecutor);
            CompletableFuture<BatchInfo> futureBatchInfo = processor.processBatch();
            BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(Status.FAIL, batchInfo.status(), "Batch status should be FAIL because one load failed.");
            assertEquals(2, batchInfo.loadingInfo().size(), "Should have load info for both files attempts.");

            LoadingInfo successInfo = batchInfo.loadingInfo().stream().filter(li -> li.fileName().equals("file_load_ok.txt")).findFirst().orElse(null);
            assertNotNull(successInfo, "Success info should be present");
            assertEquals(Status.PASS, successInfo.status());

            LoadingInfo failInfo = batchInfo.loadingInfo().stream().filter(li -> li.fileName().equals("file_load_fail.txt")).findFirst().orElse(null);
            assertNotNull(failInfo, "Fail info should be present");
            assertEquals(Status.FAIL, failInfo.status());
            assertNotNull(failInfo.failureCause());
            // The cause in LoadingInfo is the direct exception from simulateLoad
            assertTrue(failInfo.failureCause().getMessage().contains("Simulated DB error"), "Failure cause message mismatch: " + failInfo.failureCause().getMessage());
        }
    }

    // Test with empty batchData
    @Test
    void testProcessBatch_emptyBatchData() throws Exception {
        List<Path> batchData = Collections.emptyList();
        String batchId = "1";

        try (MockedStatic<LoadSimulator> mockedLoadSimulator = mockStatic(LoadSimulator.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            // ConcurrencyUtils.shutdownExecutorService for the internal loadExecutor might still be called in a finally block
            // even if no load tasks are submitted.
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(invocation -> null);

            BatchProcessor processor = new BatchProcessor(mockConfig, batchId, batchData, batchExecutor);
            CompletableFuture<BatchInfo> futureBatchInfo = processor.processBatch();
            BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(Status.PASS, batchInfo.status(), "Batch status should be PASS for an empty batch.");
            assertTrue(batchInfo.loadingInfo().isEmpty(), "Load info should be empty for an empty batch.");
            assertNull(batchInfo.failureCause());
            mockedLoadSimulator.verifyNoInteractions(); // Crucial check: simulateLoad should not be called.
        }
    }
}
