package org.gamma.plugin;

import org.gamma.metrics.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.logging.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Interface for loading a single file's data.
 */
public interface FileParser {
    /**
     * Loads data from the specified file to the target destination.
     *
     * @return LoadMetrics containing the result of the operation.
     * @throws Exception If the loading process fails. InterruptedException should be preserved.
     */
    FileInfo parseFile(Path fileName) throws Exception;

    default InputStream setupZipInputStream(InputStream inputStream) throws IOException {
        ZipInputStream zipInputStream = new ZipInputStream(inputStream);
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        if (zipEntry != null) {
            return zipInputStream;
        } else {
            try {
                inputStream.close();
            } catch (IOException var5) {
            }

            throw new IOException("Zip file is empty or could not read entry.");
        }
    }

    default Logger configureLogger(String logFile) {
        return null;
    }

    Logger getLogger();

}
    