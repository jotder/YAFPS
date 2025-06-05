package org.gamma.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Utility methods for handling concurrency, executors, and futures.
 */
public final class ConcurrencyUtils {

    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(60);

    private ConcurrencyUtils() {
    } // Prevent instantiation

    /**
     * Creates a ThreadFactory for creating named platform threads.
     */
    public static ThreadFactory createPlatformThreadFactory(final String prefix) {
        return Thread.ofPlatform().name(prefix, 500).factory();
    }

    /**
     * Gracefully shuts down an ExecutorService.
     */
    public static void shutdownExecutorService(final ExecutorService executor, final String name) {
        if (executor == null) return;
        System.out.printf("%n      Attempting graceful shutdown of executor: %s%n", name);

        executor.shutdown(); // Disable new tasks
        try {
            if (!executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                System.err.printf("      Executor %s did not terminate in %ds, attempting forceful shutdown...%n", name, SHUTDOWN_WAIT_TIMEOUT.toSeconds());
                final List<Runnable> droppedTasks = executor.shutdownNow(); // Cancel executing tasks
                System.err.printf("      Executor %s forcing shutdown. Dropped %d waiting tasks.%n", name, droppedTasks.size());

                if (!executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    System.err.printf("      Executor %s did not terminate even after forcing.%n", name);
                else
                    System.out.printf("      Executor %s terminated after forcing.%n", name);

            } else
                System.out.printf("      Executor %s terminated gracefully.%n", name);

        } catch (final InterruptedException ie) {
            System.err.printf("      Shutdown wait for executor %s interrupted. Forcing shutdown now.%n", name);
            executor.shutdownNow(); // Re-cancel if interrupted
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }

    /**
     * Waits for a list of CompletableFutures to complete, collects their results, and logs errors.
     * Handles exceptions during future completion and result retrieval.
     */
    public static <T> List<T> waitForCompletableFuturesAndCollect(
            final String levelName,
            final List<CompletableFuture<T>> futures,
            final Object identifier) {

        if (futures.isEmpty()) {
            System.out.printf("\n      No %s job to wait for (ID: %s).%n", levelName, identifier != null ? identifier : "N/A");
            return Collections.emptyList();
        }

        final List<T> results = new ArrayList<>();
        final String idStr = identifier != null ? identifier.toString() : "N/A";
        System.out.printf("\n      Waiting for %d %s job (ID: %s) using CompletableFuture.allOf()...%n", futures.size(), levelName, idStr);

        final CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allOf.join();
            System.out.printf("\n      All %s jobs (ID: %s) have completed. Collecting results...%n", levelName, idStr);
        } catch (final CancellationException e) {
            System.err.printf("!!! %s waiting (allOf) was cancelled (ID: %s).%n", levelName, idStr);
        } catch (final CompletionException e) {
            System.err.printf("!!! Unexpected error during CompletableFuture.allOf completion for %s (ID: %s): %s%n", levelName, idStr, e.getMessage());
        }

        for (final CompletableFuture<T> future : futures) {
            try {
                final T result = future.join();
                results.add(result);
            } catch (final CompletionException e) {
                System.err.printf("!!! %s job (ID: %s) completed exceptionally: %s%n", levelName, idStr, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                // Optional: e.getCause().printStackTrace(System.err);
            } catch (final CancellationException e) {
                System.err.printf("!!! %s job (ID: %s) was cancelled.%n", levelName, idStr);
            }
        }

        System.out.printf("      Finished waiting for %s (ID: %s). Collected %d results (out of %d submitted).%n%n",
                levelName, idStr, results.size(), futures.size());
        return results; // Return potentially partial results
    }
}
    