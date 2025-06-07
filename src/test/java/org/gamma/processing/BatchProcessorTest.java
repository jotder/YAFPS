package org.gamma.processing;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.gamma.metrics.MetricsManager;
import org.gamma.util.ConcurrencyUtils; // For direct calls if BatchProcessor uses it internally, though less likely to be mocked here.
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
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors; // For a real executor for the batch processor itself
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        int batchId = 1; // Non-failing processing ID

        MetricsManager.LoadInfo successfulLoad1 = new MetricsManager.LoadInfo("file1.txt", "table-x", MetricsManager.Status.PASS, Duration.ofMillis(10), "thread-load1", null);
        MetricsManager.LoadInfo successfulLoad2 = new MetricsManager.LoadInfo("file2.txt", "table-y", MetricsManager.Status.PASS, Duration.ofMillis(10), "thread-load2", null);

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
            CompletableFuture<MetricsManager.BatchInfo> futureBatchInfo = processor.processBatch();
            MetricsManager.BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS); // Wait for completion with timeout

            assertEquals(MetricsManager.Status.PASS, batchInfo.status(), "Batch status should be PASS.");
            assertEquals(batchId, batchInfo.microBatchId());
            assertNotNull(batchInfo.batchName());
            assertEquals(2, batchInfo.loadInfo().size(), "Should have load info for 2 files.");
            assertNull(batchInfo.failureCause(), "Failure cause should be null for successful batch.");

            // Verify simulateLoad was called with the correct file names and any string for table name
            mockedLoadSimulator.verify(() -> LoadSimulator.simulateLoad(eq("file1.txt"), anyString()), times(1));
            mockedLoadSimulator.verify(() -> LoadSimulator.simulateLoad(eq("file2.txt"), anyString()), times(1));
        }
    }

    // Test processing failure (SIMULATED_PROCESSING_FAILURE_MODULO)
    @Test
    void testProcessBatch_processingFailure() throws Exception {
        int failingBatchId = BatchProcessor.SIMULATED_PROCESSING_FAILURE_MODULO;
        List<Path> batchData = List.of(Paths.get("file_proc_fail.txt"));

        // No need to mock LoadSimulator as it shouldn't be called if processing fails first.
        // ConcurrencyUtils for shutting down the internal loadExecutor might still be called in a finally block.
        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {
             mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                                  .thenAnswer(invocation -> null);

            BatchProcessor processor = new BatchProcessor(mockConfig, failingBatchId, batchData, batchExecutor);
            CompletableFuture<MetricsManager.BatchInfo> futureBatchInfo = processor.processBatch();
            MetricsManager.BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(MetricsManager.Status.FAIL, batchInfo.status(), "Batch status should be FAIL due to processing error.");
            assertNotNull(batchInfo.failureCause(), "Failure cause should be set for processing failure.");
            // Check the root cause if it's wrapped by CompletionException from the supplyAsync
            Throwable rootCause = batchInfo.failureCause();
            if (rootCause instanceof java.util.concurrent.CompletionException && rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertTrue(rootCause.getMessage().contains("Simulated processing failure"), "Cause should indicate simulated processing failure.");
            assertTrue(batchInfo.loadInfo().isEmpty(), "Load info should be empty on processing failure.");
        }
    }

    // Test load failure (one file fails to load)
    @Test
    void testProcessBatch_loadFailure_oneFile() throws Exception {
        List<Path> batchData = List.of(Paths.get("file_load_ok.txt"), Paths.get("file_load_fail.txt"));
        int batchId = 1; // Non-failing processing ID

        MetricsManager.LoadInfo successfulLoad = new MetricsManager.LoadInfo("file_load_ok.txt", "table-1", MetricsManager.Status.PASS, Duration.ofMillis(10), "thread-load1", null);
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
            CompletableFuture<MetricsManager.BatchInfo> futureBatchInfo = processor.processBatch();
            MetricsManager.BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(MetricsManager.Status.FAIL, batchInfo.status(), "Batch status should be FAIL because one load failed.");
            assertEquals(2, batchInfo.loadInfo().size(), "Should have load info for both files attempts.");

            MetricsManager.LoadInfo successInfo = batchInfo.loadInfo().stream().filter(li -> li.fileName().equals("file_load_ok.txt")).findFirst().orElse(null);
            assertNotNull(successInfo, "Success info should be present");
            assertEquals(MetricsManager.Status.PASS, successInfo.status());

            MetricsManager.LoadInfo failInfo = batchInfo.loadInfo().stream().filter(li -> li.fileName().equals("file_load_fail.txt")).findFirst().orElse(null);
            assertNotNull(failInfo, "Fail info should be present");
            assertEquals(MetricsManager.Status.FAIL, failInfo.status());
            assertNotNull(failInfo.failureCause());
            // The cause in LoadInfo is the direct exception from simulateLoad
            assertTrue(failInfo.failureCause().getMessage().contains("Simulated DB error"), "Failure cause message mismatch: " + failInfo.failureCause().getMessage());
        }
    }

    // Test with empty batchData
    @Test
    void testProcessBatch_emptyBatchData() throws Exception {
        List<Path> batchData = Collections.emptyList();
        int batchId = 1;

        try (MockedStatic<LoadSimulator> mockedLoadSimulator = mockStatic(LoadSimulator.class);
             MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class)) {

            // ConcurrencyUtils.shutdownExecutorService for the internal loadExecutor might still be called in a finally block
            // even if no load tasks are submitted.
             mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                                   .thenAnswer(invocation -> null);

            BatchProcessor processor = new BatchProcessor(mockConfig, batchId, batchData, batchExecutor);
            CompletableFuture<MetricsManager.BatchInfo> futureBatchInfo = processor.processBatch();
            MetricsManager.BatchInfo batchInfo = futureBatchInfo.get(2, TimeUnit.SECONDS);

            assertEquals(MetricsManager.Status.PASS, batchInfo.status(), "Batch status should be PASS for an empty batch.");
            assertTrue(batchInfo.loadInfo().isEmpty(), "Load info should be empty for an empty batch.");
            assertNull(batchInfo.failureCause());
            mockedLoadSimulator.verifyNoInteractions(); // Crucial check: simulateLoad should not be called.
        }
    }
}
