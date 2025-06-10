package org.gamma.util;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileUtils {

    public static List<Path> getDirectoriesAsPartition(final Path baseDir, final String dirFilter) throws IOException {
        if (!Files.isDirectory(baseDir)) throw new IOException("Base dir not found: " + baseDir);
        final PathMatcher dirMatcher = (dirFilter != null && !dirFilter.isBlank())
                ? FileSystems.getDefault().getPathMatcher(dirFilter) : path -> true;
        try (var stream = Files.list(baseDir)) {
            return stream.filter(Files::isDirectory).filter(p -> dirMatcher.matches(p.getFileName())).toList();
        }
    }

    public static List<Path> listFiles(final Path sourceDir, final String fileFilter) throws IOException {
        if (!Files.isDirectory(sourceDir)) {
            System.err.printf("Warning: Dir not found: %s. Empty list.%n", sourceDir);
            return Collections.emptyList();
        }
        final PathMatcher fileMatcher = (fileFilter != null && !fileFilter.isBlank())
                ? FileSystems.getDefault().getPathMatcher(fileFilter) : path -> true;
        try (var stream = Files.list(sourceDir)) {
            return stream.filter(Files::isRegularFile).filter(p -> fileMatcher.matches(p.getFileName())).toList();
        }
    }

    public static List<List<Path>> getFileBatches(final Path partitionPath, final EtlPipelineItem conf) throws IOException {
        SourceItem pollInf = conf.sources().getFirst();
        final List<Path> pathList = listFiles(partitionPath, pollInf.fileFilter());
        final int numBuckets = Math.max(1, pollInf.numThreads()); // Uses conf.concurrency()
        if (pathList.isEmpty()) return Collections.emptyList();
        final List<List<Path>> buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) buckets.add(new ArrayList<>());
        final AtomicInteger bucketIndex = new AtomicInteger(0);
        pathList.forEach(p -> buckets.get(bucketIndex.getAndIncrement() % numBuckets).add(p));
        buckets.removeIf(List::isEmpty);
        return buckets;
    }
}
