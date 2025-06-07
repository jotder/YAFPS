package org.gamma.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;

public final class TestDataGenerator {

    private TestDataGenerator() {
        // Prevent instantiation
    }

    public static void createDummyData(List<EtlPipelineItem> configs) {
        System.out.println("Creating dummy data directories/files for testing...");
        for (EtlPipelineItem conf : configs) {
            SourceItem poll = conf.sources().getFirst();
            Path sourcePath = poll.sourceDir();
            try {
                Files.createDirectories(sourcePath);
                if (poll.useSubDirAsPartition()) {
                    for (int p = 1; p <= 3; p++) {
                        Path partPath = sourcePath.resolve("partition_" + p);
                        Files.createDirectories(partPath);
                        for (int f = 1; f <= 7; f++) {
                            String filter = poll.fileFilter();
                            String s = filter.contains("dat") ? ".dat" : filter.contains("txt") ? ".txt" : ".csv";
                            Path filePath = partPath.resolve("file_" + f + s);
                            if (!Files.exists(filePath)) Files.createFile(filePath);
                        }
                    }

                    if (poll.dirFilter() != null && !poll.dirFilter().equals("*"))
                        Files.createDirectories(sourcePath.resolve("ignored_partition"));

                } else {
                    for (int f = 1; f <= 5; f++) {
                        String ext = poll.fileFilter().contains("dat") ? ".dat" : (poll.fileFilter().contains("txt") ? ".txt" : ".csv");
                        Path filePath = sourcePath.resolve("base_file_" + f + ext);
                        if (!Files.exists(filePath)) Files.createFile(filePath);
                    }
                    if (!Files.exists(sourcePath.resolve("ignored_file.log")))
                        Files.createFile(sourcePath.resolve("ignored_file.log"));
                }
            } catch (IOException e) {
                System.err.println("Warning: Could not create dummy data for " + poll.sourceDir() + ": " + e.getMessage());
            }
        }
        System.out.println("Dummy data creation attempt finished.");
    }
}
