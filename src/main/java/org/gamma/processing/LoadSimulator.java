package org.gamma.processing;

import org.gamma.metrics.LoadingInfo;
import org.gamma.metrics.Status;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

public class LoadSimulator {

    public static final Duration SIMULATED_LOAD_DURATION = Duration.ofMillis(150);
    public static final int SIMULATED_LOAD_FAILURE_MODULO = 5;

    public static LoadingInfo simulateLoad(final String fileName, final String targetTable)
            throws InterruptedException {
        final Instant loadStart = Instant.now();
        final String threadName = Thread.currentThread().getName();

        final String stageName = "Loading " + Paths.get(fileName).getFileName() + " to " + targetTable;
        try {
            Thread.sleep(SIMULATED_LOAD_DURATION);
            if (fileName.hashCode() % SIMULATED_LOAD_FAILURE_MODULO == 0) {
                throw new RuntimeException("Simulated DB error for " + fileName);
            }
            System.out.printf("        -> %s completed on %s%n", stageName, threadName);
            return new LoadingInfo(fileName, targetTable, Status.PASS,
                    Duration.between(loadStart, Instant.now()), threadName, null);
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
