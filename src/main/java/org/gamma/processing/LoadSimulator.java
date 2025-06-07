package org.gamma.processing;

import org.gamma.metrics.MetricsManager; // For LoadInfo and Status

import java.nio.file.Path; // For Path.of()
import java.nio.file.Paths; // For Paths.get() - keep if used, or use Path.of consistently
import java.time.Duration;
import java.time.Instant;

public class LoadSimulator {

    public static final Duration SIMULATED_LOAD_DURATION = Duration.ofMillis(150);
    public static final int SIMULATED_LOAD_FAILURE_MODULO = 5;

    public static MetricsManager.LoadInfo simulateLoad(final String fileName, final String targetTable) throws InterruptedException {
        final Instant loadStart = Instant.now();
        final String threadName = Thread.currentThread().getName();
        // Using Path.of for consistency if available, otherwise Paths.get
        final String stageName = "Loading " + Paths.get(fileName).getFileName() + " to " + targetTable;
        try {
            Thread.sleep(SIMULATED_LOAD_DURATION); // Uses local constant
            if (fileName.hashCode() % SIMULATED_LOAD_FAILURE_MODULO == 0) { // Uses local constant
                throw new RuntimeException("Simulated DB error for " + fileName);
            }
            System.out.printf("        -> %s completed on %s%n", stageName, threadName);
            return new MetricsManager.LoadInfo(fileName, targetTable, MetricsManager.Status.PASS, Duration.between(loadStart, Instant.now()), threadName, null);
        } catch (final RuntimeException e) {
            System.err.printf("          ERROR during %s on %s: %s%n", stageName, threadName, e.getMessage());
            throw e;
        } catch (final InterruptedException e) {
            System.err.printf("          INTERRUPTED during %s on %s%n", stageName, threadName);
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
