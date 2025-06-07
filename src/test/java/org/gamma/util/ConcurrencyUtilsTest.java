package org.gamma.util;

// The method in ConcurrencyUtils is generic <T>, so MetricsManager.HasStatus is not strictly needed here.
// import org.gamma.metrics.MetricsManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyUtilsTest {

    // Simple record for testing. The actual ConcurrencyUtils.waitForCompletableFuturesAndCollect is generic.
    private record TestResult(String data) {}

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        // Using a cached thread pool for flexibility, or fixed if specific concurrency is needed.
        // For these tests, a small fixed pool is fine.
        executor = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void tearDown() {
        // Ensure executor is shut down after each test if it wasn't explicitly by the test logic
        if (executor != null && !executor.isTerminated()) {
            ConcurrencyUtils.shutdownExecutorService(executor, "tearDownExecutor");
        }
    }

    @Test
    void testWaitForCompletableFuturesAndCollect_emptyList() {
        List<CompletableFuture<TestResult>> futures = Collections.emptyList();
        List<TestResult> results = ConcurrencyUtils.waitForCompletableFuturesAndCollect("TestLevel", futures, "emptyTest");
        assertTrue(results.isEmpty(), "Result list should be empty for empty input list.");
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS) // Prevent test hanging indefinitely
    void testWaitForCompletableFuturesAndCollect_allSucceed() {
        List<CompletableFuture<TestResult>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> new TestResult("Result1"), executor));
        futures.add(CompletableFuture.supplyAsync(() -> {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return new TestResult("Result2");
        }, executor));

        List<TestResult> results = ConcurrencyUtils.waitForCompletableFuturesAndCollect("TestLevel", futures, "allSucceedTest");

        assertEquals(2, results.size(), "Should collect results from all succeeding futures.");
        assertTrue(results.stream().anyMatch(r -> "Result1".equals(r.data)));
        assertTrue(results.stream().anyMatch(r -> "Result2".equals(r.data)));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void testWaitForCompletableFuturesAndCollect_oneFails() {
        List<CompletableFuture<TestResult>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> new TestResult("Result1"), executor));
        futures.add(CompletableFuture.supplyAsync(() -> {
            throw new CompletionException("Simulated failure for testing", new RuntimeException("Simulated cause"));
        }, executor));
        futures.add(CompletableFuture.supplyAsync(() -> new TestResult("Result3"), executor));


        List<TestResult> results = ConcurrencyUtils.waitForCompletableFuturesAndCollect("TestLevel", futures, "oneFailsTest");

        assertEquals(2, results.size(), "Should collect results from succeeding futures only.");
        assertTrue(results.stream().anyMatch(r -> "Result1".equals(r.data)), "Result1 should be present.");
        assertTrue(results.stream().anyMatch(r -> "Result3".equals(r.data)), "Result3 should be present.");
        // Verify that the failure was logged (requires capturing System.err or a logging framework, skip for now)
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void testWaitForCompletableFuturesAndCollect_allFail() {
        List<CompletableFuture<TestResult>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> {
            throw new CompletionException("Simulated failure 1", new RuntimeException("Simulated cause 1"));
        }, executor));
        futures.add(CompletableFuture.supplyAsync(() -> {
            throw new CompletionException("Simulated failure 2", new RuntimeException("Simulated cause 2"));
        }, executor));

        List<TestResult> results = ConcurrencyUtils.waitForCompletableFuturesAndCollect("TestLevel", futures, "allFailTest");

        assertTrue(results.isEmpty(), "Result list should be empty if all futures fail.");
        // Verify that failures were logged
    }

    // Test for createPlatformThreadFactory - basic check
    @Test
    void testCreatePlatformThreadFactory() {
        ThreadFactory factory = ConcurrencyUtils.createPlatformThreadFactory("MyTestThread-");
        assertNotNull(factory);
        Thread newThread = factory.newThread(() -> {});
        assertNotNull(newThread);
        assertTrue(newThread.getName().startsWith("MyTestThread-"), "Thread name should have the correct prefix.");
    }

    // Test for shutdownExecutorService - basic check (hard to test full behavior without race conditions)
    @Test
    void testShutdownExecutorService_nullExecutor() {
        // Should not throw an exception
        assertDoesNotThrow(() -> {
            ConcurrencyUtils.shutdownExecutorService(null, "NullTestExecutor");
        });
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS) // Increased timeout for executor shutdown
    void testShutdownExecutorService_normalShutdown() throws InterruptedException {
        // Use the class-level executor for this test, or a new one if preferred for isolation
        ExecutorService localExecutor = Executors.newFixedThreadPool(1);
        localExecutor.submit(() -> {
            try {
                Thread.sleep(50); // Simulate some work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        ConcurrencyUtils.shutdownExecutorService(localExecutor, "NormalShutdownTest");
        assertTrue(localExecutor.isTerminated(), "Executor should be terminated after shutdown.");
    }
}
