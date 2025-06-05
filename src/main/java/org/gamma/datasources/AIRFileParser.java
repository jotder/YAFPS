package org.gamma.datasources;

import org.gamma.config.YamlSourceConfigAdapter;
import org.gamma.processing.BatchProcessor;
import org.gamma.processing.StatusWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simulates loading a file with potential delays and failures.
 */
public class AIRFileParser extends BatchProcessor {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    DateTimeFormatter eventDateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected StatusWriter statusHandler;

    public AIRFileParser(YamlSourceConfigAdapter config) throws IOException {
        super(config);
        logger = this.configureLogger(config.sourceName() + "LOG_FILE");
        this.statusHandler = new StatusWriter(config.fileInfo(), config.routeInfo(), this.logger);

    }

    public synchronized StatusWriter getStatusHandler() {
        return this.statusHandler;
    }

    @Override
    public String createOutFileName(List<Path> paths) {
        String[] n1 = paths.getFirst().getFileName().toFile().getName().split("_");
        String n2 = paths.getLast().toFile().getName().split("_")[2];
        return n1[0] + "_" + n1[1] + "_" + n1[2] + ".." + n2 + "_" + n1[3].substring(0, n1[3].indexOf(".")) + ".csv";
    }

    // Keep simulation constants here or move to a dedicated SimulationConfig class
    private static final int SIMULATED_LOAD_FAILURE_MODULO = 100;
    private static final Duration SIMULATED_LOAD_DURATION = Duration.ofMillis(1500);


//    public Map<String, Object> enrich(List<String> fields, long recIndex, String fileName) throws Exception {
//        Map<String, Object> rec = new LinkedHashMap();
//
//        try {
//            for (String fn : this.sourceMD.keySet()) {
//                AIRFileParser.FieldMetadata f = (AIRFileParser.FieldMetadata) this.sourceMD.get(fn);
//                if (f != null && f.fieldSeq < fields.size()) {
//                    rec.put(fn, fields.get(f.fieldSeq));
//                }
//            }
//
//            int idx = ((AIRFileParser) this.sourceMD.get("event_timestamp_enrich")).fieldSeq;
//            LocalDateTime event_timestamp_enrich = this.convertEpochToTimestamp(Long.parseLong(fields.get(idx)));
//            rec.put("event_timestamp_enrich", event_timestamp_enrich.format(this.formatter));
//            rec.put("rec_seq_no", String.valueOf(recIndex));
//            rec.put("file_name", String.valueOf(fileName));
//            rec.put("event_date", event_timestamp_enrich.format(this.eventDateFmt));
//            return rec;
//        } catch (Exception e) {
//            System.out.println(fields);
//            e.printStackTrace();
//            Logger var10000 = this.getLogger();
//            Level var10001 = Level.SEVERE;
//            String var10002 = e.getMessage();
//            var10000.log(var10001, var10002 + ", rec size:" + fields.size());
//            throw e;
//        }
//    }

    @Override
    public String getRecSortIndicatorFld() {
        return "";
    }

//    public FileInfo parseFile(Path filePath) throws InterruptedException {
//        final String stageName = "Parsing " + Paths.get(filePath.toString()).getFileName() + " to ";
//
//        final Instant loadStart = Instant.now();
//        final String threadName = Thread.currentThread().getName();
//
//        try {
//            Thread.sleep(SIMULATED_LOAD_DURATION);
//
//            if (filePath.hashCode() % SIMULATED_LOAD_FAILURE_MODULO == 0)
//                throw new RuntimeException("Simulated DB connection error for " + filePath);
//
////            String fileID, String dataSource, String fileName, long fileSize, Timestamp lastModTs,
////                    String sourceFileUrl, long recCount, long failCount, String fileType, String task, Timestamp startTs,
////            long duration, String recordStart, String recordEnd, String message
//
////            System.out.printf("        -> %s completed successfully on %s%n", stageName, threadName);
//            String fileName = filePath.toFile().getName();
//            return new FileInfo(Utils.getFileID(config.sourceName(), fileName), config.sourceName(), fileName, filePath.toFile().length(),
//                    Timestamp.from(Instant.ofEpochMilli(filePath.toFile().lastModified())),
//                    filePath.toString(), 0, 0, "SOURCE", "PARSE", Timestamp.from(Instant.ofEpochMilli(Instant.now().toEpochMilli())),
//                    0, "recordStart", "recordEnd", null);
//        } catch (InterruptedException e) {
////            System.err.printf("          INTERRUPTED during %s on %s%n", stageName, threadName);
//            Thread.currentThread().interrupt();
//            throw e;
//        } catch (RuntimeException e) {

    /// /            System.err.printf("          ERROR during %s on %s: %s%n", stageName, threadName, e.getMessage());
//            // Let BatchProcessor handle creating failed metrics from the exception
//            throw e;
//        }
//        // No need to return failed metrics here, exception propagation handles it.
//    }
    @Override
    protected String getPartition(Map<String, Object> rec) {
        return "";
    }


//    public ParseInfo processFile(PollInfo pollFileInf, SplitMergeBatchHandler splitMergeBatchHandler, MatrixBuilder matrixBuilder) {
//        long fileProcessStart = System.nanoTime();
//        Timestamp ts = Timestamp.from(Instant.now());
//        long parseCount = 0L;
//        long failCount = 0L;
//        long txCount = 0L;
//        String startRecord = "";
//        String endRecord = "";
//        String fileErrorMsg = null;
//        splitMergeBatchHandler.startFile(pollFileInf);
//        Map<String, Integer> recordErrorMsg = new LinkedHashMap<>();
//
//        try (
//                InputStream fileInputStream = new FileInputStream(pollFileInf.url());
//                InputStream decompressStream = (pollFileInf.url().endsWith(".gz") ?
//                        new GZIPInputStream(fileInputStream) : (pollFileInf.url().endsWith(".zip") ?
//                        this.setupZipInputStream(fileInputStream) : fileInputStream));
//                Reader reader = new InputStreamReader(decompressStream);
//                BufferedReader bufferedReader = new BufferedReader(reader);
//        ) {
//            Stream<String> lines = bufferedReader.lines();
//            int headerLine = config.getHeaderLines(); // this.getHeaderLines(pollFileInf);
//            Path errDir = config.getErrorDir(); // this.getProperties().getProperty("ERR_DIR");
//            Path errFilePath = errDir.resolve(pollFileInf.fileName() + ".err.csv");
//
//            try (BufferedWriter errWriter = Files.newBufferedWriter(errFilePath)) {
//                int lineCount = 0;
//                Iterator<String> lineIterator = lines.iterator();
//
//                while (lineIterator.hasNext()) {
//                    String line = lineIterator.next();
//                    ++lineCount;
//                    if (lineCount > headerLine) {
//                        try {
//                            char escapeEnclosed = 0;
//                            List<String> srcFields = this.parseLine(line, escapeEnclosed, '\'');
//                            ++parseCount;
//                            if (srcFields.size() != 125)
//                                throw new Exception("Invalid record with field count " + srcFields.size());
//
//                            Map<String, Object> rec = this.enrich(srcFields, parseCount, pollFileInf.fileName());
//                            ++txCount;
//                            List<String> txMD = this.getFieldsNames("TX");
//                            splitMergeBatchHandler.write(rec, txMD);
//                            String seqFld = this.getRecSortIndicatorFld();
//                            if (seqFld != null) {
//                                Object val = rec.get(seqFld);
//                                if (val != null) {
//                                    String v = val.toString();
//                                    if (startRecord.isEmpty()) startRecord = v;
//                                    if (endRecord.isEmpty()) endRecord = v;
//                                    int c = this.compareTo(v, startRecord);
//                                    if (c < 0) startRecord = v;
//                                    else endRecord = v;
//                                }
//                            }
//
//                            try {
//                                if (matrixBuilder != null)
//                                    matrixBuilder.populate(rec);
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        } catch (Exception e) {
//                            ++failCount;
//                            errWriter.write(line + System.lineSeparator());
//                            String message = e.getMessage() != null ? e.getMessage() : "Unknown record error";
//                            recordErrorMsg.merge(message, 1, Integer::sum);
//                        }
//                    }
//                }
//
//                if (!recordErrorMsg.isEmpty()) {
//                    System.out.println(recordErrorMsg);
////                    Logger var10000 = this.getLogger();
////                    Level var10001 = Level.WARNING;
//                    String var10002 = pollFileInf.fileName();
////                    var10000.log(var10001, "Record errors in file " + var10002 + ": " + String.valueOf(recordErrorMsg));
//                }
//
//                if (failCount > 0L) {
//                    fileErrorMsg = (String) recordErrorMsg.keySet().stream().findFirst().orElse("Record processing issue(s)");
//                }
//            } catch (IOException e) {
//                fileErrorMsg = "Error writing error file: " + e.getMessage();
//                e.printStackTrace();
////                this.getLogger().log(Level.SEVERE, "Error writing error file for " + pollFileInf.fileName(), e);
//            }
//
//            if (failCount == 0L && fileErrorMsg == null) {
//                try {
//                    Files.deleteIfExists(errFilePath);
//                } catch (IOException e) {
////                    this.getLogger().log(Level.WARNING, "Could not delete empty error file: " + String.valueOf(errFilePath), e);
//                }
//            }
//        } catch (ZipException ze) {
//            fileErrorMsg = "Zip file processing error: " + ze.getMessage();
////            this.getLogger().log(Level.SEVERE, "ZipException for file " + pollFileInf.fileName(), ze);
//            failCount = parseCount;
//            txCount = 0L;
//        } catch (IOException e) {
//            fileErrorMsg = "File read/access error: " + e.getMessage();
////            this.getLogger().log(Level.SEVERE, "IOException for file " + pollFileInf.fileName(), e);
//        } catch (Exception e) {
//            fileErrorMsg = "Unexpected error processing file: " + e.getMessage();
////            this.getLogger().log(Level.SEVERE, "Unexpected error for file " + pollFileInf.fileName(), e);
//        }
//
//        if (failCount > 0L) {
//            fileErrorMsg = recordErrorMsg.entrySet().stream().findFirst().map((ex) -> {
//                String var10000 = ex.getKey();
//                return var10000 + " :: " + ex.getValue() + " times";
//            }).orElse("Record process error");
//        }
//
//        String status;
//        if (fileErrorMsg != null && !fileErrorMsg.isEmpty()) {
//            if (txCount == 0L && parseCount > 0L)
//                status = "FAIL";
//            else if (txCount > 0L)
//                status = "PART_PASS";
//            else
//                status = "FAIL";
//        } else {
//            status = "PASS";
//            if (parseCount == 0L && txCount == 0L)
//                fileErrorMsg = "Zero records processed";
//        }
//
//        if (parseCount != txCount + failCount && status.equals("PASS")) {

    /// /            this.getLogger().log(Level.WARNING, "Count mismatch (Parse:" + parseCount + ", Tx:" + txCount + ", Fail:" + failCount + ") for " + pollFileInf.fileName());
//        }
//
//        long now = System.nanoTime();
//        long duration = (now - fileProcessStart) / 1000000L;
//        if (duration <= 0L) {
//            System.out.println("duration can not be 0 ");
//        }
//
//        return new ParseInfo(pollFileInf, parseCount, failCount, "SOURCE", "PROCESS", ts, duration, startRecord, endRecord, fileErrorMsg == null ? "" : fileErrorMsg);
//    }

//    public int compareTo(String current, String reference) {
//        return current.compareTo(reference);
//    }

    //    Map<String, FieldMetadata> sourceMD;
//    ArrayList<String> fields = null;

//    public List<String> getFieldsNames(String name) {
//        if (name.equalsIgnoreCase("source")) {
//            return config.getSourceMD().values().stream().map(FieldMetadata::fieldName).toList();
//        } else {
//            if (this.fields == null) {
//                this.fields = this.getListFromCSV("this.props.getProperty(\"ENRICHED_FIELDS\")");
//            }
//
//            return this.fields;
//        }
//    }

//    private ArrayList<String> getListFromCSV(String csv) {
//        String[] x = csv.split(",");
//        ArrayList<String> list = new ArrayList<>(x.length);
//        Arrays.stream(x).forEach((e) -> list.add(e.trim()));
//        return list;
//    }
//
    public Map<String, Object> enrich(List<String> fields, long recIndex, String fileName) throws Exception {
        Map<String, Object> rec = new LinkedHashMap<>();
//        Map<String, FieldMetadata> metaData = config.getSourceMD();
        try {
//            for (String fn : metaData.keySet()) {
//                FieldMetadata f = metaData.get(fn);
//                if (f != null && f.fieldSeq() < fields.size())
//                    rec.put(fn, fields.get(f.fieldSeq()));
//            }
//
//            int idx = (metaData.get("event_timestamp_enrich")).fieldSeq();
//            LocalDateTime event_timestamp_enrich = this.convertEpochToTimestamp(Long.parseLong((String) fields.get(idx)));
//            rec.put("event_timestamp_enrich", event_timestamp_enrich.format(this.formatter));
            rec.put("rec_seq_no", String.valueOf(recIndex));
            rec.put("file_name", String.valueOf(fileName));
//            rec.put("event_date", event_timestamp_enrich.format(this.eventDateFmt));
            return rec;
        } catch (Exception e) {
            System.out.println(fields);
            e.printStackTrace();
//            String msg = e.getMessage();
//            msg.log(Level.SEVERE, msg + ", rec size:" + fields.size());
            throw e;
        }
    }


//    List<String> parseLine(String line, char encloseChar, char escapeChar) {
//        List<String> fields = new ArrayList<>();
//        StringBuilder currentField = new StringBuilder();
//        boolean insideField = false;
//        boolean handleEnclose = false;
//
//        for (int i = 0; i < line.length(); ++i) {
//            char ch = line.charAt(i);
//            if (ch == ',') {
//                if (insideField) {
//                    handleEnclose = true;
//                    currentField.append(ch);
//                } else {
//                    String fieldValue = currentField.toString().trim();
//                    if (handleEnclose) {
//                        fieldValue = escapeCsvField(fieldValue);
//                        handleEnclose = false;
//                    }
//                    fields.add(fieldValue);
//                    currentField.setLength(0);
//                }
//            } else if (ch == encloseChar)
//                insideField = !insideField;
//            else
//                currentField.append(ch);
//        }
//
//        fields.add(currentField.toString());
//        return fields;
//    }

    //    private static String escapeCsvField(String field) {
//        if (!field.contains(",") && !field.contains("\"") && !field.contains("\n"))
//            return field;
//        else {
//            field = field.replace("\"", "\"\"");
//            return "\"" + field + "\"";
//        }
//    }
//
//    public String getRecSortIndicatorFld() {
//        return "date_time";
//    }
//
    public LocalDateTime convertEpochToTimestamp(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }


//    Logger getLogger();
}
    