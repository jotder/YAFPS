package org.gamma.processing;

import org.gamma.metrics.LoadingInfo;
import org.gamma.metrics.Status;
import org.gamma.plugin.FileLoader;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

/**
 * Simulates loading a file with potential delays and failures.
 */
public class SimulatedFileLoader implements FileLoader {

    // Keep simulation constants here or move to a dedicated SimulationConfig class
    private static final int SIMULATED_LOAD_FAILURE_MODULO = 100;
    private static final Duration SIMULATED_LOAD_DURATION = Duration.ofMillis(1500);

    @Override
    public LoadingInfo parseFile(String fileName, String targetTable) throws InterruptedException {
        final String stageName = "Loading " + Paths.get(fileName).getFileName() + " to " + targetTable;

        final Instant loadStart = Instant.now();
        final String threadName = Thread.currentThread().getName();

        try {
            Thread.sleep(SIMULATED_LOAD_DURATION);

            if (fileName.hashCode() % SIMULATED_LOAD_FAILURE_MODULO == 0)
                throw new RuntimeException("Simulated DB connection error for " + fileName);

//            System.out.printf("        -> %s completed successfully on %s%n", stageName, threadName);
            return new LoadingInfo(fileName, targetTable, Status.PASS, Duration.between(loadStart, Instant.now()), threadName, null);

        } catch (RuntimeException e) {
//            System.err.printf("          ERROR during %s on %s: %s%n", stageName, threadName, e.getMessage());
            // Let BatchProcessor handle creating failed metrics from the exception
            throw e;
        } catch (InterruptedException e) {
//            System.err.printf("          INTERRUPTED during %s on %s%n", stageName, threadName);
            Thread.currentThread().interrupt();
            throw e;
        }
        // No need to return failed metrics here, exception propagation handles it.
    }
}
    