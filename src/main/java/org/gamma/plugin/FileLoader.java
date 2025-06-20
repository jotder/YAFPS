package org.gamma.plugin;

import org.gamma.metrics.LoadingInfo;

/**
 * Interface for loading a single file's data.
 */
public interface FileLoader {
    /**
     * Loads data from the specified file to the target destination.
     *
     * @return LoadMetrics containing the result of the operation.
     * @throws Exception If the loading process fails. InterruptedException should be preserved.
     */
    LoadingInfo parseFile(String fileName, String targetTable) throws Exception;
}
    