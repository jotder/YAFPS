package org.gamma.processing;

import org.gamma.config.YamlSourceConfigAdapter;
// import org.gamma.processing.BatchProcessor; // No longer needed as ProcessingResult is top-level
// import org.gamma.processing.ProcessingResult; // Will use MetricsManager.ProcessingResult
import org.gamma.metrics.MetricsManager; // Added import
import org.gamma.metrics.FileInfo; // Ensure this is imported for fileInfoList
import org.gamma.metrics.RouteInfo;
import org.gamma.metrics.Status;
import org.gamma.metrics.StatusHelper;
import org.gamma.plugin.FileParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import static org.gamma.util.Utils.escapeCsvField;


public abstract class AbstractBatchFileParser implements FileParser {
    Path outDir;
    String dims;
    String measures;
    String option;
    Path matrixFilePath;
    public MatrixBuilder matrixHandler = null;
    // private final SourceConfig config; // OLD
    protected final YamlSourceConfigAdapter config; // NEW
    protected Logger logger; // Made protected

    public AbstractBatchFileParser(YamlSourceConfigAdapter config) { // Constructor name fixed
        this.outDir = config.outputDir();
        this.dims = config.getDimensions();
        this.measures = config.getMeasures();
        this.option = config.getOptions();
        this.matrixFilePath = outDir.resolve("matrix");
        this.config = config;
    }

    public abstract Map<String, Object> enrich(List<String> var1, long var2, String var4) throws Exception;

    public abstract String getRecSortIndicatorFld();

    public abstract StatusWriter getStatusHandler();

    public Logger getLogger() {
        return logger;
    }

    // Keep simulation constants here or move elsewhere
    private static final int SIMULATED_PROCESSING_FAILURE_MODULO = 10;

    // ProcessingResult is now a top-level class: org.gamma.processing.ProcessingResult - comment to be removed or updated

    public MetricsManager.ProcessingResult processPhase(int batchId, String batchName, List<Path> paths) throws // Return type changed
            IOException {
        Instant batchStart = Instant.now(); // Reinstated
        String currentThreadName = Thread.currentThread().getName(); // Reinstated

        System.out.printf("      %s: Starting processing phase on T %s...%n", batchName, currentThreadName);

        String outFileName = "out" + config.sourceName() + Math.random() + ".csv";
        try {
            outFileName = createOutFileName(paths);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Files.createDirectories(matrixFilePath);
        if ("ALL".equals(option) || "MATRIX".equals(option))
            matrixHandler = new MatrixBuilder(dims, measures, matrixFilePath + File.separator + "sum_" + outFileName, config.isOutFileHeader());

        List<String> txFieldNames = config.getFieldsNames("TX");
        boolean keepHeader = config.isOutFileHeader();

        BatchMergeSplitWriter batchHandler = new BatchMergeSplitWriter(outDir, outFileName, txFieldNames, option, keepHeader);
        String recStart = "";
        String recEnd = "";
        long totValRecCount = 0L;
        long totBadRecCount = 0L;

        List<FileInfo> fileInfoList = new ArrayList<>();
        // List<RouteInfo> routeInfList = new ArrayList<>(); // Commented out if not used for Map return

        for (Path path : paths) {
            try {

                FileInfo info = processFile(path, batchHandler, matrixHandler);

                fileInfoList.add(info);
                List<List<Map.Entry<String, Map<String, Long>>>> routeInfo = batchHandler.getRouteStatus(info.fileName());

//                long x = routeInfo.values().stream().mapToLong(Long::longValue).sum();
//                if (x != info.recCount() - info.failCount())
//                    System.out.println("ERROR : pass - fail != sum of routed ! " + x + " " + info.recCount() + " " + info.failCount());

                String msg = info.fileName();
                String m = "\t-> " + msg + ":\t\t\t\t\t" + info.recCount() + "\t\t" + info.failCount() + "\t\t" + info.duration() + "\t\t" + info.status();
                System.out.println(m);
                logger.log(Level.WARNING, m);

//                routeInfo.forEach((edge, count) ->
//                        routeInfList.add(StatusHelper.createRouteRecord(config.getSourceName(), edge.srcFile(), edge.outFile(), count)));

                if (info.recordStart().compareTo(recStart) > 0)
                    recStart = info.recordStart();

                if (info.recordEnd().compareTo(recEnd) < 0)
                    recEnd = info.recordEnd();

                totValRecCount += info.recCount();
                totBadRecCount += info.failCount();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            Collection<MergedFileWriter> mergedFileInf = batchHandler.partitionWriters.values();
            AtomicLong recordCount = new AtomicLong(0L);
            StatusWriter statusWriter = this.getStatusHandler();

            mergedFileInf.forEach((cw) -> {
                try {

                    recordCount.set(recordCount.get() + cw.getRecordCount());

                    FileInfo merged = StatusHelper.createMergedRecord(config.sourceName(), cw);

                    fileInfoList.add(merged);
                    PrintStream printStream = System.out;
                    String msg = cw.mFlePath.toFile().getName();
                    printStream.println("\t-> " + msg + "\t" + merged.fileType() + "\t" + cw.getRecordCount() + "\t" + cw.mFlePath);
                } catch (Exception x) {
                    x.printStackTrace();
                }

            });
            if (totValRecCount - totBadRecCount != recordCount.get()) {
                System.out.println("WARNING :: Valid src count doesn't match with merged !!!");
            }

            if (matrixHandler != null) {
                matrixHandler.writeMatrix();
                matrixHandler.close();

                //                FileInfo mfi = Utils.UtilsgetFileInf(dataSource, Path.of(matrixFilePath + File.separator + "sum_" + outFileName));

                String matrixTable = config.getMatrixTableName();
                boolean matrixLoading = config.isMatrixLoad();
                if (matrixLoading) {
                    statusWriter.loadDataToDatabase(matrixHandler.fileUrl, matrixTable);
                    FileInfo mLoadStatus = StatusHelper.createMatricesInfo();//
                    fileInfoList.add(mLoadStatus);
                }
            }

            statusWriter.insertFileStatus(fileInfoList);
//            statusWriter.insertRouteStatus(routeInfList);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("???????????? Status Log Issue ?????????????");
        }

        batchHandler.close();

//        return fileInfoList;


//        final Map<String, String> filesToParse = new LinkedHashMap<>();
//        try {
//            for (Path p : paths) {
//                FileInfo result = new AIRFileParser(config).parseFile(p);
//                filesToParse.put(p.toString(), "table-" + (p.hashCode() % 2 + 1));
//            }
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        if (batchId % SIMULATED_PROCESSING_FAILURE_MODULO == 0)
            throw new RuntimeException("Simulated processing failure in " + batchName);

        // Simulate work (replace with actual logic)
        final Map<String, String> filesToLoad = new LinkedHashMap<>(); // Reinstated
        for (Path p : paths) { // Reinstated
            filesToLoad.put(p.toString(), "table-" + (p.hashCode() % 2 + 1)); // Reinstated
        }

//        System.out.printf("      %s: Processing phase completed successfully.%n", batchName);
        return new MetricsManager.ProcessingResult(batchId, batchName, batchStart, currentThreadName, paths, filesToLoad, fileInfoList); // Use MetricsManager.ProcessingResult
    }

    public abstract String createOutFileName(List<Path> paths);

    @Override
    public FileInfo parseFile(Path fileName) throws Exception {
        return null;
    }

    @Override
    public InputStream setupZipInputStream(InputStream inputStream) throws IOException {
        return FileParser.super.setupZipInputStream(inputStream);
    }

    @Override
    public Logger configureLogger(String logFile) {
        return FileParser.super.configureLogger(logFile);
    }


    FileInfo processFile(Path path, BatchMergeSplitWriter batchMergeSplitWriter, MatrixBuilder matrixBuilder) {
        Instant startMoment = Instant.now();
        long parseCount = 0L;
        long failCount = 0L;
        long txCount = 0L;
        String startRecord = "";
        String endRecord = "";
        String errorMsg = null;
        batchMergeSplitWriter.startFile(path);
        Map<String, Integer> recErrMsg = new LinkedHashMap<>();

        try (InputStream fileInputStream = new FileInputStream(path.toFile());
             InputStream decompressStream = path.getFileName().endsWith(".gz") ? new GZIPInputStream(fileInputStream)
                     : (path.getFileName().endsWith(".zip") ? this.setupZipInputStream(fileInputStream) : fileInputStream);
             Reader reader = new InputStreamReader(decompressStream);
             BufferedReader bufferedReader = new BufferedReader(reader)) {

            Stream<String> lines = bufferedReader.lines();
            int headerLine = config.headerLines();
            Path errFile = config.errorDir().resolve(path.toFile().getName() + ".err.csv");

            try (BufferedWriter errWriter = Files.newBufferedWriter(errFile)) {
                int lineCount = 0;
                Iterator<String> lineIterator = lines.iterator();

                while (lineIterator.hasNext()) {
                    String line = lineIterator.next();
                    ++lineCount;
                    if (lineCount > headerLine) {
                        try {
                            char escapeEnclosed = 0;
                            List<String> srcFields = this.parseLine(line, escapeEnclosed, '\'');
                            ++parseCount;

                            if (srcFields.size() != config.srcFieldSize())
                                throw new Exception("Invalid record with field count " + srcFields.size());

                            String fileName = path.toFile().getName();
                            Map<String, Object> rec = enrich(srcFields, parseCount, fileName);
                            ++txCount;


                            String partition = this.getPartition(rec);

                            List<String> txMD = config.getFieldsNames("TX");
                            batchMergeSplitWriter.write(fileName, partition, rec, txMD);

                            String seqFld = this.getRecSortIndicatorFld();
                            if (seqFld != null) {
                                Object val = rec.get(seqFld);
                                if (val != null) {
                                    String v = val.toString();
                                    if (startRecord.isEmpty())
                                        startRecord = v;

                                    if (endRecord.isEmpty())
                                        endRecord = v;

                                    int c = this.compareTo(v, startRecord);
                                    if (c < 0)
                                        startRecord = v;
                                    else
                                        endRecord = v;
                                }
                            }

                            try {
                                if (matrixBuilder != null)
                                    matrixBuilder.populate(rec);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } catch (Exception e) {
                            ++failCount;
                            errWriter.write(line + System.lineSeparator());
                            String message = e.getMessage() != null ? e.getMessage() : "Unknown record error";
                            recErrMsg.merge(message, 1, Integer::sum);
                        }
                    }
                }

                if (!recErrMsg.isEmpty()) {
                    System.out.println(recErrMsg);
                    logger.log(Level.WARNING, "Record errors in file " + path.toFile().getName() + ": " + recErrMsg);
                }

                if (failCount > 0L)
                    errorMsg = recErrMsg.keySet().stream().findFirst().orElse("Record processing issue(s)");

            } catch (IOException e) {
                errorMsg = "Error writing error file: " + e.getMessage();
                e.printStackTrace();
                logger.log(Level.SEVERE, "Error writing error file for " + path.toFile().getName(), e);
            }

            if (failCount == 0L && errorMsg == null) {
                try {
                    Files.deleteIfExists(errFile);
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Could not delete empty error file: " + String.valueOf(errFile), e);
                }
            }

        } catch (ZipException ze) {
            errorMsg = ze.getMessage() == null ? "Zip file processing error" : ze.getMessage();
            logger.log(Level.SEVERE, "ZipException for file " + path.toFile().getName(), errorMsg);
        } catch (IOException e) {
            errorMsg = e.getMessage() == null ? "File read/access error" : e.getMessage();
            logger.log(Level.SEVERE, "IOException for file " + path.toFile().getName(), e);
        } catch (Exception e) {
            errorMsg = e.getMessage() == null ? "Unexpected error processing file" : e.getMessage();
            logger.log(Level.SEVERE, "Unexpected error for file " + path.toFile().getName(), e);
        }

        if (failCount > 0L) {
            errorMsg = recErrMsg.entrySet().stream().findFirst().map((ex) -> {
                String msg = ex.getKey();
                return msg + " :: " + ex.getValue() + " times";
            }).orElse("Record process error");
        }

        String status;
        if (txCount > 0L) {
            if (txCount == parseCount)  // txCount == parseCount
                status = "PASS";
            else                        // txCount < parseCount
                status = "PARTIAL";
        } else                          // txCount == 0
            if (parseCount == 0L) {     // 0 record
                if (errorMsg == null) {
                    status = "PASS";
                    errorMsg = "0 record file";
                } else
                    status = "FAIL";
            } else                      // parseCount >  0 & txCount == 0
                status = "FAIL";

        if (parseCount != txCount + failCount && status.equals("PASS"))
            logger.log(Level.WARNING, "Count mismatch (Parse:" + parseCount + ", Tx:" + txCount + ", Fail:" + failCount + ") for "
                                      + path.toFile().getName());

        long duration = Duration.between(startMoment, Instant.now()).toSeconds();
        return StatusHelper.createFileInfo(path, config.sourceName(), parseCount, failCount, Timestamp.from(startMoment),
                duration, Status.PASS, startRecord, endRecord, errorMsg);
    }
//    Path path, String dataSource, long parseCount, long failCount,  Timestamp startTime, int duration,
//    Status status, String startRecord,String endRecord, String fileErrorMsg

    protected abstract String getPartition(Map<String, Object> rec);


    int compareTo(String current, String reference) {
        return current.compareTo(reference);
    }

    List<String> parseLine(String line, char encloseChar, char escapeChar) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideField = false;
        boolean handleEnclose = false;

        for (int i = 0; i < line.length(); ++i) {
            char ch = line.charAt(i);
            if (ch == ',') {
                if (insideField) {
                    handleEnclose = true;
                    currentField.append(ch);
                } else {
                    String fieldValue = currentField.toString().trim();
                    if (handleEnclose) {
                        fieldValue = escapeCsvField(fieldValue);
                        handleEnclose = false;
                    }

                    fields.add(fieldValue);
                    currentField.setLength(0);
                }
            } else if (ch == encloseChar)
                insideField = !insideField;
            else
                currentField.append(ch);
        }

        fields.add(currentField.toString());
        return fields;
    }
}
