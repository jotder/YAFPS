package org.gamma.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * Utility methods for file system operations like listing files and directories.
 */
public final class Utils {

    private Utils() {
    }

    /**
     * Lists immediate subdirectories matching a glob pattern.
     */
    public static List<Path> getDirectoriesAsPartition(final Path baseDir, final String filter) throws IOException {

        final FileSystem fs = FileSystems.getDefault();
        final PathMatcher dirMatcher = (filter != null && !filter.isBlank())
                ? fs.getPathMatcher(filter)
                : path -> true;

        try (var stream = Files.list(baseDir)) {
            return stream
                    .filter(Files::isDirectory)
                    .filter(p -> dirMatcher.matches(p.getFileName()))
                    .toList();
        }
    }

    /**
     * Lists files within a directory matching a glob pattern (non-recursive).
     */
    public static List<Path> listFiles(final Path rootDir, final String fileFilter) throws IOException {
        if (!Files.isDirectory(rootDir)) {
            System.err.printf("Warning: Source directory does not exist or is not a directory: %s. Returning empty list.%n", rootDir);
            return Collections.emptyList();
        }
        final FileSystem fs = FileSystems.getDefault();
        final PathMatcher fileMatcher = (fileFilter != null && !fileFilter.isBlank())
                ? fs.getPathMatcher(fileFilter)
                : _ -> true;

        try (var stream = Files.list(rootDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> fileMatcher.matches(p.getFileName()))
                    .toList();
        }
    }

    public static List<Path> findFiles(Path rootDir, String fileFilter, int maxDepth) throws IOException {
        FileSystem fs = FileSystems.getDefault();
        final PathMatcher fileMatcher = (fileFilter != null && !fileFilter.isBlank())
                ? fs.getPathMatcher(fileFilter)
                : _ -> true;
        try (Stream<Path> paths = Files.find(
                rootDir,
                maxDepth,
                (path, p) ->
                        Files.isRegularFile(path) && fileMatcher.matches(path.getFileName())
        ) //path.toString().endsWith(".properties"))
        ) {
            return paths.collect(Collectors.toList());
        }
    }

    public static Properties loadProperties(Path source) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(source.toFile()));
        return props;
    }

    public static String escapeCsvField(String field) {
        if (field == null) {
            return "";
        } else {
            boolean mustQuote = field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r");
            String escaped = field.replace("\"", "\"\"");
            return mustQuote ? "\"" + escaped + "\"" : escaped;
        }

    }

    String escapeCsv(String value) {
        if (value == null) {
            return "";
        } else {
            return !value.contains(",") && !value.contains("\n") && !value.contains("\"") ? value : "\"" + value.replace("\"", "\"\"") + "\"";
        }
    }

    /**
     * Distributes a list of files into a number of batches determined by concurrency,
     * using round-robin assignment.
     */
    public static List<List<Path>> getFileBatches(Path partitionPath, String fileFilter, int batchSize) throws IOException {
        final List<Path> pathList = listFiles(partitionPath, fileFilter);

        //Todo check the backup directory if files are already available, indicates it's duplicate file or processed earlier run
        // filter out in case of duplicate files

        return splitIntoBuckets(pathList, batchSize);
    }

    public static List<List<Path>> splitIntoBuckets(List<Path> paths, int bucketSize) {
        List<List<Path>> buckets = new ArrayList<>();
        int total = paths.size();
        for (int i = 0; i < total; i += bucketSize) {
            int end = Math.min(i + bucketSize, total);
            buckets.add(paths.subList(i, end));
        }
        return buckets;
    }

    public static List<List<Path>> splitIntoBuckets(List<Path> paths, long targetBucketSizeBytes) throws IOException {
        List<List<Path>> buckets = new ArrayList<>();
        List<Path> currentBucket = new ArrayList<>();
        long currentBucketSize = 0;

        for (Path path : paths) {
            long fileSize = Files.size(path);

            // If adding this file would exceed the limit, start a new bucket
            if (currentBucketSize + fileSize > targetBucketSizeBytes && !currentBucket.isEmpty()) {
                buckets.add(currentBucket);
                currentBucket = new ArrayList<>();
                currentBucketSize = 0;
            }

            currentBucket.add(path);
            currentBucketSize += fileSize;
        }

        // Add the final bucket if not empty
        if (!currentBucket.isEmpty()) {
            buckets.add(currentBucket);
        }

        return buckets;
    }

    static String removeEnclQuotes(String input) {
        return input != null && input.startsWith("\"") && input.endsWith("\"") ? input.substring(1, input.length() - 1) : input;
    }

//    static Models.PollFileInf getFileInf(String dataSource, Path filePath) throws Exception {
//        String fileName = filePath.toFile().getName();
//        File f = filePath.toFile();
//        Timestamp lastModified = Timestamp.from(Instant.ofEpochMilli(f.lastModified()));
//        String fileID = getFileID(dataSource + fileName);
//        return new Models.PollFileInf(fileID, dataSource, fileName, f.length(), lastModified, f.getCanonicalPath());
//    }

    public static String getFileID(String dataSource, String filename) {
        CRC32 crc = new CRC32();
        crc.update((dataSource + filename).getBytes());
        return String.valueOf(crc.getValue());
    }

//    public static PollInfo getFileInf(String dataSource, Path filePath) throws Exception {
//        String fileName = filePath.toFile().getName();
//        File f = filePath.toFile();
//        Timestamp lastModified = Timestamp.from(Instant.ofEpochMilli(f.lastModified()));
//        String fileID = getFileID(dataSource, fileName);
//        return new PollInfo(fileID, dataSource, fileName, Status.PASS, f.length(), lastModified, f.getCanonicalPath());
//    }

//    static void backupBucket(List<Models.FileStatus> bktStatus) {
//        Path backDir = Paths.get(this.getProperties().getProperty("BACKUP_DIR"));
//        Path errDir = Paths.get(this.getProperties().getProperty("ERR_DIR"));
//        boolean errBackup = Boolean.parseBoolean(this.getProperties().getProperty("ERR_BACKUP"));
//
//        try {
//            Files.createDirectories(backDir);
//            Files.createDirectories(errDir);
//            bktStatus.forEach((fInf) -> {
//                if (fInf.fileType().equalsIgnoreCase("SOURCE")) {
//                    try {
//                        String fileName = fInf.pollFileInf().fileName();
//                        Path sourcePath = Path.of(fInf.pollFileInf().sourceFileUrl());
//                        String subDir = sourcePath.getName(sourcePath.getNameCount() - 2).toString();
//                        Path targetPath;
//                        if (!fInf.status().equalsIgnoreCase("PASS")) {
//                            targetPath = Paths.get(errDir.toString(), subDir);
//                        } else {
//                            targetPath = Paths.get(backDir.toString(), subDir);
//                        }
//
//                        Files.createDirectories(targetPath);
//                        if (!fInf.status().equalsIgnoreCase("PASS") && errBackup) {
//                            Files.copy(sourcePath, targetPath.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                } else {
//                    System.out.print("");
//                }
//
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
}

    