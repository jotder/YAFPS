package org.gamma;

import org.gamma.config.AppConfig;
import org.gamma.config.EtlPipelineItem;
import org.gamma.metrics.DataSourceInfo;
import org.gamma.metrics.ExecutionInfo;
import org.gamma.metrics.Status;
import org.gamma.util.ConcurrencyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class YAFPSTest {

    @Mock
    private AppConfig mockAppConfig;
    @Mock
    private EtlPipelineItem mockEtlItem1;
    @Mock
    private EtlPipelineItem mockEtlItem2;
    // DataSourceProcessor is instantiated within YAFPS.execute(), so we don't mock its instance directly here.
    // We mock the results of ConcurrencyUtils.waitForCompletableFuturesAndCollect.

    private YAFPS yafpsInstance;

    @BeforeEach
    void setUp() {
        // Default behavior for AppConfig - can be overridden in specific tests
        lenient().when(mockAppConfig.etlPipelines()).thenReturn(List.of(mockEtlItem1, mockEtlItem2));
        // Ensure EtlPipelineItem mocks also have pipelineName stubbed for logging inside YAFPS.execute -> supplyAsync -> catch
        lenient().when(mockEtlItem1.pipelineName()).thenReturn("pipe1");
        lenient().when(mockEtlItem2.pipelineName()).thenReturn("pipe2");

        yafpsInstance = new YAFPS(mockAppConfig);
    }

    private DataSourceInfo createDummyDsInfo(String sourceName, Status status) {
        return new DataSourceInfo(sourceName + "_id", sourceName, status, Duration.ofSeconds(1), "thread-ds", Collections.emptyList(),
                status == Status.FAIL ? new Throwable("Simulated DS fail for " + sourceName) : null);
    }

    @Test
    void testExecute_successfulProcessingOfMultiplePipelines() throws Exception {
        when(mockAppConfig.etlPipelines()).thenReturn(List.of(mockEtlItem1, mockEtlItem2)); // Explicitly set for this test
        yafpsInstance = new YAFPS(mockAppConfig); // Re-initialize with specific config for this test

        DataSourceInfo dsInfo1 = createDummyDsInfo("pipe1", Status.PASS);
        DataSourceInfo dsInfo2 = createDummyDsInfo("pipe2", Status.PASS);

        // Use try-with-resources for MockedStatic
        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class);
             MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class)) { // To control executor creation

            ExecutorService mockExecutorService = mock(ExecutorService.class);
            ThreadFactory mockThreadFactory = mock(ThreadFactory.class);

            // Stubbing the static factory methods
            mockedExecutors.when(() -> Executors.newFixedThreadPool(anyInt(), any(ThreadFactory.class)))
                    .thenReturn(mockExecutorService);
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString()))
                    .thenReturn(mockThreadFactory);

            // This is the key mock: define what the collection of futures will return.
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(
                            eq("DataSource"),
                            anyList(),
                            isNull()
                    ))
                    .thenAnswer(invocation -> {
                        List<?> futuresPassed = invocation.getArgument(1);
                        assertEquals(2, futuresPassed.size(), "Should have received 2 futures for processing.");
                        // Simulate that these futures (which would wrap DataSourceProcessor calls) resolve to our mock data
                        return List.of(dsInfo1, dsInfo2);
                    });

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(inv -> null); // Do nothing for shutdown in test


            ExecutionInfo execInfo = yafpsInstance.execute();

            assertNotNull(execInfo);
            assertEquals(2, execInfo.dataSourceInfo().size());
            assertTrue(execInfo.dataSourceInfo().contains(dsInfo1));
            assertTrue(execInfo.dataSourceInfo().contains(dsInfo2));

            // Verify that an executor was created with the correct concurrency and then shut down.
            mockedExecutors.verify(() -> Executors.newFixedThreadPool(eq(2), eq(mockThreadFactory)));
            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.shutdownExecutorService(eq(mockExecutorService), eq("DataSourceExecutor")));
        }
    }

    @Test
    void testExecute_noPipelines() throws Exception {
        when(mockAppConfig.etlPipelines()).thenReturn(Collections.emptyList());
        yafpsInstance = new YAFPS(mockAppConfig);

        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class);
             MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class)) {

            ExecutorService mockExecutorService = mock(ExecutorService.class);
            ThreadFactory mockThreadFactory = mock(ThreadFactory.class);

            // Adjust concurrency expectation for no pipelines. Max(1,0) = 1.
            mockedExecutors.when(() -> Executors.newFixedThreadPool(eq(1), any(ThreadFactory.class)))
                    .thenReturn(mockExecutorService);
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString()))
                    .thenReturn(mockThreadFactory);

            // waitForCompletableFuturesAndCollect will be called with an empty list of futures.
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("DataSource"), anyList(), isNull()))
                    .thenAnswer(invocation -> {
                        List<?> futures = invocation.getArgument(1);
                        assertTrue(futures.isEmpty(), "Future list should be empty.");
                        return Collections.emptyList();
                    });
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString()))
                    .thenAnswer(inv -> null);

            ExecutionInfo execInfo = yafpsInstance.execute();

            assertNotNull(execInfo);
            assertTrue(execInfo.dataSourceInfo().isEmpty());

            mockedConcurrencyUtils.verify(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("DataSource"), eq(Collections.emptyList()), isNull()));
        }
    }

    @Test
    void testExecute_onePipelineFailsToProcess() throws Exception {
        when(mockAppConfig.etlPipelines()).thenReturn(List.of(mockEtlItem1)); // Only one item
        yafpsInstance = new YAFPS(mockAppConfig);

        DataSourceInfo failedDsInfo = createDummyDsInfo("pipe1", Status.FAIL);

        try (MockedStatic<ConcurrencyUtils> mockedConcurrencyUtils = mockStatic(ConcurrencyUtils.class);
             MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class)) {

            ExecutorService mockExecutorService = mock(ExecutorService.class);
            ThreadFactory mockThreadFactory = mock(ThreadFactory.class);

            mockedExecutors.when(() -> Executors.newFixedThreadPool(anyInt(), any(ThreadFactory.class)))
                    .thenReturn(mockExecutorService);
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.createPlatformThreadFactory(anyString()))
                    .thenReturn(mockThreadFactory);

            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.waitForCompletableFuturesAndCollect(eq("DataSource"), anyList(), isNull()))
                    .thenAnswer(invocation -> {
                        List<?> futures = invocation.getArgument(1);
                        assertEquals(1, futures.size());
                        return List.of(failedDsInfo); // Simulate this one future resolving to a failed DataSourceInfo
                    });
            mockedConcurrencyUtils.when(() -> ConcurrencyUtils.shutdownExecutorService(any(ExecutorService.class), anyString())).thenAnswer(inv -> null);

            ExecutionInfo execInfo = yafpsInstance.execute();

            assertNotNull(execInfo);
            assertEquals(1, execInfo.dataSourceInfo().size());
            assertTrue(execInfo.dataSourceInfo().contains(failedDsInfo));
            assertEquals(Status.FAIL, execInfo.dataSourceInfo().getFirst().status());
        }
    }

    @Test
    void testConstructor_nullConfig() {
        assertThrows(NullPointerException.class, () -> new YAFPS(null),
                "Constructor should throw NullPointerException if AppConfig is null.");
    }

    @Test
    void testConstructor_emptyPipelinesWarning() {
        // This test requires capturing System.err. For simplicity, we'll assume the warning is logged
        // and focus on the fact that YAFPS can be created.
        // If capturing output was critical, a library like System Rules or custom setup would be needed.
        when(mockAppConfig.etlPipelines()).thenReturn(Collections.emptyList());
        assertDoesNotThrow(() -> new YAFPS(mockAppConfig),
                "Constructor should handle empty ETL pipelines list without throwing an exception.");
        // To verify the warning, one might need to redirect System.err, which is more involved.
    }
}
