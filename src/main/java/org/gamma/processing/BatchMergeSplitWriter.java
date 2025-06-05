package org.gamma.processing;

import org.gamma.util.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchMergeSplitWriter {
//    String dataSource;
    Path outputDir;
    String mode;
    boolean keepHeader;
    List<String> headerFields;
    long overallStartTime;
    Map<String, MergedFileWriter> partitionWriters = new HashMap<>();
    Set<String> fileNamePartition = new TreeSet<>();
    Path srcFile;
    private String mergedFileName;

    public BatchMergeSplitWriter(Path outDir, String mergedFileName, List<String> headerFields, String mode, boolean keepHeader) {
        this.mode = mode;
        this.outputDir = outDir;
        this.mergedFileName = mergedFileName;
        this.headerFields = headerFields;
        this.overallStartTime = System.currentTimeMillis();
        this.keepHeader = keepHeader;
    }

    public void write(String sFileName, String partition, Map<String, Object> rec, List<String> metaData) throws IOException {
        List<String> txFields = new ArrayList<>();

        for (String fn : metaData) {
            Object o = rec.get(fn);
            if (o != null) {
                txFields.add(Utils.escapeCsvField(o.toString()));
            } else {
                System.out.println("Hang on, some issue ");
            }
        }

        String csvLine = String.join(",", txFields);

        if (partition != null && !partition.isBlank())
            outputDir = outputDir.resolve(partition);

        mergedFileName = getMergedFileName(sFileName, partition);

        Files.createDirectories(outputDir);
        Path mFlePath = outputDir.resolve(mergedFileName);

        MergedFileWriter mergedFileWriter = this.partitionWriters.computeIfAbsent(mergedFileName, (_) -> {
            try {
                return new MergedFileWriter(mFlePath, this.headerFields, this.keepHeader);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create writer for partition " + mFlePath, e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        mergedFileWriter.writeLine(sFileName, csvLine);
    }

    private String getMergedFileName(String sFileName, String partition) {
        if (partition != null && !partition.isBlank())
            return partition + "_" + sFileName;
        else
            return mergedFileName = sFileName;
    }

    public void startFile(Path srcFile) {
        this.srcFile = srcFile;
    }

    public void close() {
        AtomicInteger closedCount = new AtomicInteger();
        List<String> closeErrors = new ArrayList<>();

        try {
            if (this.partitionWriters != null) {
                this.partitionWriters.values().forEach((writerx) -> {
                    if (!writerx.isClosed()) {
                        try {
                            writerx.close();
                            closedCount.getAndIncrement();
                        } catch (IOException e) {
                            String errorMsg = String.format("Error closing writer for partition (%s): %s", writerx.getmFlePath().getFileName(), e.getMessage());
                            closeErrors.add(errorMsg);
                        }
                    }

                    System.out.print("");
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } //
        finally {
            assert this.partitionWriters != null;

            for (MergedFileWriter writer : this.partitionWriters.values()) {
                if (writer.isClosed()) {
                    try {
                        writer.close();
                        closedCount.getAndIncrement();
                    } catch (IOException e) {
                        String errorMsg = String.format("Error closing writer for partition (%s): %s", writer.getmFlePath().getFileName(), e.getMessage());
                        closeErrors.add(errorMsg);
                    }
                }
            }

            if (!closeErrors.isEmpty())
                System.out.println("Encountered errors during final writer cleanup: " + String.join("; ", closeErrors));
        }
    }

    List<List<Map.Entry<String, Map<String, Long>>>> getRouteStatus(String sFileName) {
        List<String> partitions = getPartitionNames(sFileName);

        List<List<Map.Entry<String, Map<String, Long>>>> x = partitions.stream()
                .map(p -> partitionWriters.entrySet().stream()
                        .filter(e -> e.getKey().equals(getMergedFileName(p, sFileName)))
                        .map(e -> Map.entry(e.getKey(), e.getValue().edgeStatus))
                        .toList())
                .toList();

        System.out.println(x);
        return x;
    }

    Map<String, List<String>> fileNamePartitions = new HashMap<>();

    private List<String> getPartitionNames(String sFileName) {
        return fileNamePartitions.get(sFileName);
    }
}