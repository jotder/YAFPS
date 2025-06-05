package org.gamma.config;

import org.apache.avro.Schema; // Import Avro Schema
// Potentially import your RecordReaderFactory interface if SourceConfig is to provide it
// import com.gamma.processing.record.RecordReaderFactory;
// import com.gamma.processing.record.CsvRecordReaderFactory; // Example implementation

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SourceConfig {
    private static final Logger LOGGER = Logger.getLogger(SourceConfig.class.getName());
    private final Properties props;
    private final Schema dataRecordSchema; // Primary metadata: the Avro schema

    // Map<String, FieldMetadata> sourceMD; // This is now derived or removed.
    // If derived, make it final and initialize in constructor.

    // Optional: Configuration for the RecordReader, e.g., CSV delimiter, quote char
    // private final Map<String, String> recordReaderConfig;

    char sep = File.pathSeparatorChar;

    public SourceConfig(Properties props) {
        this.props = props;

        // 1. Load the Avro Schema - This is now the primary metadata source
        String avroSchemaPathKey = "AVRO_DATA_SCHEMA_PATH";
        String avroSchemaPath = safeGet(avroSchemaPathKey, "conf/sources/air/air_data_schema.avsc"); // Example default
        if (avroSchemaPath == null || avroSchemaPath.trim().isEmpty()) {
            LOGGER.log(Level.SEVERE, "Avro schema path property ('" + avroSchemaPathKey + "') is not configured.");
            throw new IllegalArgumentException("Avro data schema path is required.");
        }
        try {
            this.dataRecordSchema = new Schema.Parser().parse(new File(avroSchemaPath));
            LOGGER.log(Level.INFO, "Successfully loaded Avro data schema from: " + avroSchemaPath);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load Avro data schema from path: " + avroSchemaPath, e);
            throw new RuntimeException("Could not initialize SourceConfig: Avro schema loading failed.", e);
        }

        // 2. The old sourceMD from CSV is no longer read directly.
        // this.sourceMD = readFieldMetadata("conf/sources/air/air_md.csv"); // REMOVE THIS

        // 3. If you still need a structure like sourceMD for other purposes, derive it from dataRecordSchema.
        // this.sourceMD = deriveFieldMetadataFromAvro(this.dataRecordSchema);

        // 4. Initialize any RecordReader specific configurations from props
        // this.recordReaderConfig = new HashMap<>();
        // this.recordReaderConfig.put("csv.delimiter", safeGet("CSV_DELIMITER", ","));
        // this.recordReaderConfig.put("csv.skipHeader", safeGet("CSV_SKIP_HEADER", "true"));
    }

    public Schema getDataRecordSchema() {
        return dataRecordSchema;
    }

    // public Map<String, String> getRecordReaderConfig() {
    //     return Collections.unmodifiableMap(recordReaderConfig);
    // }

    // If you choose to provide a RecordReaderFactory from SourceConfig:
    // public RecordReaderFactory getRecordReaderFactory() {
    //     String readerType = safeGet("RECORD_READER_TYPE", "CSV");
    //     if ("CSV".equalsIgnoreCase(readerType)) {
    //         return new CsvRecordReaderFactory(); // Assuming you have this class
    //     }
    //     throw new IllegalArgumentException("Unsupported record reader type: " + readerType);
    // }


    // getSourceMD() would be removed or return the derived map.
    /*
    public Map<String, FieldMetadata> getSourceMD() {
        if (this.sourceMD == null) {
            // Or throw exception if it's expected to be always available
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(sourceMD);
    }
    */

    // The readFieldMetadata method is no longer needed if Avro schema is the source.
    /*
    public Map<String, FieldMetadata> readFieldMetadata(String filePath) {
        // ... old implementation ...
        // THIS METHOD SHOULD BE REMOVED or REPURPOSED if you were, for example,
        // generating an Avro schema *from* such a CSV (less common).
    }
    */

    // Example: Deriving a FieldMetadata-like structure from Avro Schema
    // (You'd need to adapt your FieldMetadata class or decide if it's still needed)
    /*
    private Map<String, FieldMetadata> deriveFieldMetadataFromAvro(Schema schema) {
        Map<String, FieldMetadata> derived = new LinkedHashMap<>();
        int seq = 0;
        for (Schema.Field field : schema.getFields()) {
            String typeName = getAvroFieldTypeName(field.schema());
            boolean isNullable = field.schema().getType() == Schema.Type.UNION &&
                                 field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
            // Assuming FieldMetadata constructor: FieldMetadata(int seq, String name, String type, String nullableStr)
            derived.put(field.name(), new FieldMetadata(++seq, field.name(), typeName, String.valueOf(isNullable)));
        }
        return derived;
    }

    private String getAvroFieldTypeName(Schema fieldSchema) {
        if (fieldSchema.getType() == Schema.Type.UNION) {
            // Find the first non-null type in the union for simplicity
            return fieldSchema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .map(s -> s.getType().getName() + (s.getLogicalType() != null ? " ("+s.getLogicalType().getName()+")" : ""))
                    .orElse("union");
        }
        return fieldSchema.getType().getName() + (fieldSchema.getLogicalType() != null ? " ("+fieldSchema.getLogicalType().getName()+")" : "");
    }
    */


    public String sourceID() {
        return safeGet("DATA_SOURCE", "NoID");
    }

    public String sourceName() {
        return safeGet("DATA_SOURCE_NAME", sourceID()); // Default to sourceID if specific name not set
    }

    // ... (other getter methods remain similar, but their backing properties might change) ...

    public int srcFieldSize() {
        // This should now reliably come from the Avro schema
        return this.dataRecordSchema.getFields().size();
    }

    public List<String> getFieldsNames(String nameType) {
        // "source" fields are directly from the Avro schema
        if ("source".equalsIgnoreCase(nameType)) {
            return this.dataRecordSchema.getFields().stream().map(Schema.Field::name).toList();
        } else if ("TX".equalsIgnoreCase(nameType)) { // For enriched/transformed fields
            return getListFromCSV(safeGet("ENRICHED_FIELDS", ""));
        }
        LOGGER.log(Level.WARNING, "Unknown field name type requested: " + nameType);
        return Collections.emptyList();
    }

    private ArrayList<String> getListFromCSV(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return new ArrayList<>();
        }
        // Ensure robustness for empty strings from split if the CSV string ends with a comma
        return new ArrayList<>(Arrays.asList(csv.split(",", -1))).stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty()) // Optional: remove empty fields resulting from ",," or trailing/leading commas
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    private String safeGet(String key, String defaultValue) {
        String val = props.getProperty(key);
        // Consider logging if a key is not found and defaultValue is used, for diagnostics
        if (val == null) {
            // LOGGER.log(Level.FINER, "Property '" + key + "' not found, using default value: " + defaultValue);
            return defaultValue;
        }
        String trimmedVal = val.trim();
        return (trimmedVal.isEmpty() || "null".equalsIgnoreCase(trimmedVal)) ? defaultValue : trimmedVal;
    }

    private int parseInt(String value, int defaultValue) {
        if (value == null) return defaultValue;
        String trimmedValue = value.trim();
        if (trimmedValue.isEmpty() || "null".equalsIgnoreCase(trimmedValue)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(trimmedValue);
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Failed to parse integer value: '" + value + "'. Using default: " + defaultValue, e);
            return defaultValue;
        }
    }

    public Path sourceDir() throws IOException {
        Path sourceDirPath = Paths.get(safeGet("SOURCE_DIR", "data" + sep + "source"));
        if (!Files.isDirectory(sourceDirPath)) {
            String message = "Source directory does not exist or is not a directory: " + sourceDirPath;
            LOGGER.log(Level.SEVERE, message);
            throw new IOException(message);
        }
        return sourceDirPath;
    }

    // ... other path methods: errorDir(), outputDir(), fileInfo(), routeInfo()

    public Path errorDir() {
        return Paths.get(safeGet("ERR_DIR", "data" + sep + "error"));
    }

    public Path outputDir() {
        return Paths.get(safeGet("OUT_DIR", "data" + sep + "output"));
    }

    public Path fileInfo() {
        return Paths.get(safeGet("FILE_INFO_TABLE", "data" + sep + "status" + sep + "file_info.csv"));
    }

    public Path routeInfo() {
        return Paths.get(safeGet("ROUTE_INFO_TABLE", "data" + sep + "status" + sep + "route_info.csv"));
    }


    public boolean isDirAsPartition() {
        return Boolean.parseBoolean(safeGet("DIR_AS_PARTITION", "false"));
    }

    public boolean isActive() {
        return Boolean.parseBoolean(safeGet("ACTIVE", "true"));
    }

    public boolean isOutFileHeader() {
        return Boolean.parseBoolean(safeGet("OUTFILE_HEADER", "true"));
    }

    public int maxConcurrency() {
        return parseInt(safeGet("NUM_THREADS", "1"), 1);
    }

    public int batchSize() {
        return parseInt(safeGet("BATCH_SIZE", "5"), 5);
    }

    public int headerLines() {
        // This might be specific to the RecordReader configuration, e.g., "csv.skipHeader"
        return parseInt(safeGet("HEADER_LINES_TO_SKIP", "1"), 1);
    }


    public String getMeasures() {
        return safeGet("MATRIX_MEASURES", "");
    }

    public String getDimensions() {
        return safeGet("MATRIX_DIMS", "");
    }

    public String getOptions() {
        return safeGet("PROCESS_OPTION", "");
    }

    public String getMatrixTableName() {
        return safeGet("MATRIX_TABLE", null);
    }

    public boolean isMatrixLoad() {
        return Boolean.parseBoolean(safeGet("MATRIX_LOAD", "false"));
    }
}