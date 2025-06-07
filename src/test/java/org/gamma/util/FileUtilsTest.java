package org.gamma.util;

import org.gamma.config.EtlPipelineItem;
import org.gamma.config.SourceItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
// Potentially import mockito if complex mocking of EtlPipelineItem is needed
// import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FileUtilsTest {

    @TempDir
    Path tempDir; // JUnit Jupiter annotation for creating a temporary directory

    Path baseDir;

    @BeforeEach
    void setUp() throws IOException {
        // Create a structure within tempDir for testing
        // e.g., baseDir = tempDir.resolve("base"); Files.createDirectories(baseDir);
        // Specific setup is done in each test method to avoid interference
    }

    // Tests for getDirectoriesAsPartition
    // Test cases:
    // 1. No directories
    // 2. Multiple directories, no filter
    // 3. Multiple directories, with filter matching some
    // 4. Multiple directories, with filter matching none
    // 5. Base directory does not exist (should throw IOException)

    @Test
    void testGetDirectoriesAsPartition_noDirectories() throws IOException {
        baseDir = tempDir.resolve("testGetDirectories_noDirs");
        Files.createDirectories(baseDir);
        List<Path> dirs = FileUtils.getDirectoriesAsPartition(baseDir, null);
        assertTrue(dirs.isEmpty(), "Should find no directories.");
    }

    @Test
    void testGetDirectoriesAsPartition_multipleDirsNoFilter() throws IOException {
        baseDir = tempDir.resolve("testGetDirectories_multiNoFilter");
        Files.createDirectories(baseDir);
        Files.createDirectory(baseDir.resolve("dir1"));
        Files.createDirectory(baseDir.resolve("dir2"));
        Files.createFile(baseDir.resolve("file.txt")); // Should be ignored

        List<Path> dirs = FileUtils.getDirectoriesAsPartition(baseDir, null);
        assertEquals(2, dirs.size(), "Should find 2 directories.");
        assertTrue(dirs.contains(baseDir.resolve("dir1")));
        assertTrue(dirs.contains(baseDir.resolve("dir2")));
    }

    @Test
    void testGetDirectoriesAsPartition_multipleDirsWithFilter() throws IOException {
        baseDir = tempDir.resolve("testGetDirectories_multiWithFilter");
        Files.createDirectories(baseDir);
        Files.createDirectory(baseDir.resolve("data_dir1"));
        Files.createDirectory(baseDir.resolve("other_dir2"));
        Files.createDirectory(baseDir.resolve("data_dir3"));

        List<Path> dirs = FileUtils.getDirectoriesAsPartition(baseDir, "glob:data_dir*");
        assertEquals(2, dirs.size(), "Should find 2 directories matching filter.");
        assertTrue(dirs.stream().anyMatch(p -> p.endsWith("data_dir1")));
        assertTrue(dirs.stream().anyMatch(p -> p.endsWith("data_dir3")));
    }

    @Test
    void testGetDirectoriesAsPartition_filterMatchingNone() throws IOException {
        baseDir = tempDir.resolve("testGetDirectories_filterMatchesNone");
        Files.createDirectories(baseDir);
        Files.createDirectory(baseDir.resolve("data_dir1"));
        Files.createDirectory(baseDir.resolve("other_dir2"));

        List<Path> dirs = FileUtils.getDirectoriesAsPartition(baseDir, "glob:non_matching_filter*");
        assertTrue(dirs.isEmpty(), "Should find no directories with non-matching filter.");
    }

    @Test
    void testGetDirectoriesAsPartition_baseDirDoesNotExist() {
        Path nonExistentBaseDir = tempDir.resolve("nonExistentBase");
        assertThrows(IOException.class, () -> {
            FileUtils.getDirectoriesAsPartition(nonExistentBaseDir, null);
        }, "Should throw IOException if base directory does not exist.");
    }


    // Tests for listFiles
    // Test cases:
    // 1. No files in directory
    // 2. Multiple files, no filter
    // 3. Multiple files, with filter matching some
    // 4. Multiple files, with filter matching none
    // 5. Source directory does not exist (should return empty list and log warning)

    @Test
    void testListFiles_noFiles() throws IOException {
        baseDir = tempDir.resolve("testListFiles_noFiles");
        Files.createDirectories(baseDir);
        List<Path> files = FileUtils.listFiles(baseDir, null);
        assertTrue(files.isEmpty(), "Should find no files.");
    }

    @Test
    void testListFiles_multipleFilesNoFilter() throws IOException {
        baseDir = tempDir.resolve("testListFiles_multiNoFilter");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.csv"));
        Files.createDirectory(baseDir.resolve("subdir")); // Should be ignored

        List<Path> files = FileUtils.listFiles(baseDir, null);
        assertEquals(2, files.size(), "Should find 2 files.");
        assertTrue(files.stream().anyMatch(p -> p.endsWith("file1.txt")));
        assertTrue(files.stream().anyMatch(p -> p.endsWith("file2.csv")));
    }

    @Test
    void testListFiles_multipleFilesWithFilter() throws IOException {
        baseDir = tempDir.resolve("testListFiles_multiWithFilter");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.csv"));
        Files.createFile(baseDir.resolve("another.txt"));

        List<Path> files = FileUtils.listFiles(baseDir, "glob:*.txt");
        assertEquals(2, files.size(), "Should find 2 .txt files.");
        assertTrue(files.stream().anyMatch(p -> p.endsWith("file1.txt")));
        assertTrue(files.stream().anyMatch(p -> p.endsWith("another.txt")));
    }

    @Test
    void testListFiles_filterMatchingNone() throws IOException {
        baseDir = tempDir.resolve("testListFiles_filterMatchesNone");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.csv"));

        List<Path> files = FileUtils.listFiles(baseDir, "glob:*.log");
        assertTrue(files.isEmpty(), "Should find no files with non-matching filter.");
    }

    @Test
    void testListFiles_sourceDirDoesNotExist() throws IOException {
        Path nonExistentDir = tempDir.resolve("nonExistent");
        List<Path> files = FileUtils.listFiles(nonExistentDir, null);
        assertTrue(files.isEmpty(), "Should return empty list for non-existent directory.");
        // Also check for logged warning (requires more advanced test setup, skip for now)
    }

    // Tests for getFileBatches
    // Test cases:
    // 1. No files to batch
    // 2. Fewer files than numBuckets
    // 3. Files exactly numBuckets
    // 4. More files than numBuckets, testing distribution
    // 5. numBuckets is 1
    // 6. conf object needs to be mocked for fileFilter and numThreads

    // Helper to create a mock EtlPipelineItem for getFileBatches
    private EtlPipelineItem createMockEtlConfig(String fileFilter, int numThreads) {
        // Using actual Path for sourceDir, as SourceItem expects Path, not String for dir
        SourceItem sourceItem = new SourceItem("testSource", baseDir, true, "glob:*", fileFilter, numThreads, 10, null, null, null, null);
        // For simplicity, assuming EtlPipelineItem can be constructed with a list of one sourceItem.
        // If EtlPipelineItem is complex, use Mockito:
        // EtlPipelineItem mockConfig = mock(EtlPipelineItem.class);
        // SourceItem mockSourceItem = mock(SourceItem.class);
        // when(mockSourceItem.fileFilter()).thenReturn(fileFilter);
        // when(mockSourceItem.numThreads()).thenReturn(numThreads);
        // when(mockConfig.sources()).thenReturn(List.of(mockSourceItem));
        // return mockConfig;
        // Manual construction if simple enough:
        return new EtlPipelineItem("testPipeline", "Test Pipeline", List.of(sourceItem), null, null, null, null);
    }

    @Test
    void testGetFileBatches_noFiles() throws IOException {
        baseDir = tempDir.resolve("testGetFileBatches_noFiles");
        Files.createDirectories(baseDir);
        EtlPipelineItem conf = createMockEtlConfig("*.txt", 3);
        List<List<Path>> batches = FileUtils.getFileBatches(baseDir, conf);
        assertTrue(batches.isEmpty(), "Should return no batches for no files.");
    }

    @Test
    void testGetFileBatches_fewerFilesThanBuckets() throws IOException {
        baseDir = tempDir.resolve("testGetFileBatches_fewerFiles");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.txt"));

        EtlPipelineItem conf = createMockEtlConfig("*.txt", 3); // 3 buckets
        List<List<Path>> batches = FileUtils.getFileBatches(baseDir, conf);
        assertEquals(2, batches.size(), "Should have 2 batches as there are 2 files.");
        assertEquals(1, batches.get(0).size());
        assertEquals(1, batches.get(1).size());
    }

    @Test
    void testGetFileBatches_exactFilesAsBuckets() throws IOException {
        baseDir = tempDir.resolve("testGetFileBatches_exactFiles");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.txt"));
        Files.createFile(baseDir.resolve("file3.txt"));

        EtlPipelineItem conf = createMockEtlConfig("*.txt", 3); // 3 buckets
        List<List<Path>> batches = FileUtils.getFileBatches(baseDir, conf);
        assertEquals(3, batches.size());
        batches.forEach(batch -> assertEquals(1, batch.size()));
    }

    @Test
    void testGetFileBatches_moreFilesThanBuckets() throws IOException {
        baseDir = tempDir.resolve("testGetFileBatches_moreFiles");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.txt"));
        Files.createFile(baseDir.resolve("file3.txt"));
        Files.createFile(baseDir.resolve("file4.txt"));
        Files.createFile(baseDir.resolve("file5.txt"));

        EtlPipelineItem conf = createMockEtlConfig("*.txt", 3); // 3 buckets
        List<List<Path>> batches = FileUtils.getFileBatches(baseDir, conf);
        assertEquals(3, batches.size(), "Should have 3 batches.");
        // Check distribution: e.g., 2, 2, 1
        long countOfTwo = batches.stream().filter(b -> b.size() == 2).count();
        long countOfOne = batches.stream().filter(b -> b.size() == 1).count();
        assertTrue((countOfTwo == 2 && countOfOne == 1) , "Distribution should be 2,2,1 for 5 files in 3 buckets");
        assertEquals(5, batches.stream().mapToLong(List::size).sum(), "Total files in batches should match original number.");
    }

    @Test
    void testGetFileBatches_numBucketsIsOne() throws IOException {
        baseDir = tempDir.resolve("testGetFileBatches_oneBucket");
        Files.createDirectories(baseDir);
        Files.createFile(baseDir.resolve("file1.txt"));
        Files.createFile(baseDir.resolve("file2.txt"));
        Files.createFile(baseDir.resolve("file3.txt"));

        EtlPipelineItem conf = createMockEtlConfig("*.txt", 1); // 1 bucket
        List<List<Path>> batches = FileUtils.getFileBatches(baseDir, conf);
        assertEquals(1, batches.size(), "Should have 1 batch.");
        assertEquals(3, batches.get(0).size(), "The single batch should contain all 3 files.");
    }
}
