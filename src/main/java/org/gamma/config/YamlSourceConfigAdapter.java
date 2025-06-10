//package org.gamma.config;
//
//import org.apache.avro.Schema;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//
///**
// * Adapts a YAML-defined DataSourceDefinition and an Avro Schema to provide
// * a configuration interface similar to the old Properties-based SourceConfig.
// * This allows BatchProcessor and other components to transition smoothly.
// */
//public class YamlSourceConfigAdapter {
//    private static final Logger LOGGER = Logger.getLogger(YamlSourceConfigAdapter.class.getName());
//
//    private final DataSourceDefinition definition;
//    private final Schema dataRecordSchema;
//    private final Path pipelineConfigBasePath; // Base path for resolving relative paths from YAML
//
//    // File.separator is platform-dependent, good for constructing paths.
//    private final String separator = File.separator;
//
//    public YamlSourceConfigAdapter(DataSourceDefinition definition, Schema dataRecordSchema, Path pipelineConfigBasePath) {
//        if (definition == null) {
//            throw new IllegalArgumentException("DataSourceDefinition cannot be null.");
//        }
//        if (dataRecordSchema == null) {
//            throw new IllegalArgumentException("DataRecordSchema cannot be null.");
//        }
//        this.definition = definition;
//        this.dataRecordSchema = dataRecordSchema;
//        // Ensure pipelineConfigBasePath is a directory, or fallback to current dir
//        this.pipelineConfigBasePath = (pipelineConfigBasePath != null && Files.isDirectory(pipelineConfigBasePath))
//                ? pipelineConfigBasePath
//                : Paths.get(".").toAbsolutePath().normalize();
//        LOGGER.log(Level.CONFIG, "YamlSourceConfigAdapter initialized for source: {0} with base path: {1}",
//                new Object[]{definition.sourceId(), this.pipelineConfigBasePath});
//    }
//
//    private Path resolvePath(String pathString, String defaultRelativePath) {
//        String effectivePathString = (pathString != null && !pathString.trim().isEmpty())
//                ? pathString.trim()
//                : defaultRelativePath;
//        Path path = Paths.get(effectivePathString);
//        if (path.isAbsolute()) {
//            return path.normalize();
//        }
//        return pipelineConfigBasePath.resolve(path).normalize();
//    }
//
//    public Schema getDataRecordSchema() {
//        return dataRecordSchema;
//    }
//
//    public String sourceID() {
//        return definition.sourceId();
//    }
//
//    public String sourceName() {
//        return definition.sourceName() != null ? definition.sourceName() : definition.sourceId();
//    }
//
//    public Path sourceDir() throws IOException {
//        // Default subdirectory if not specified, e.g., "data/input/[sourceId]"
//        Path dir = resolvePath(definition.sourceDir(), "data" + separator + "input" + separator + definition.sourceId());
//        if (!Files.isDirectory(dir)) {
//            // Decide on behavior: auto-create or throw. For now, throw.
//            LOGGER.log(Level.WARNING, "Source directory does not exist or is not a directory: {0}. Attempting to create.", dir);
//            Files.createDirectories(dir); // Let's try to create it.
//            // throw new IOException("Source directory does not exist or is not a directory: " + dir);
//        }
//        return dir;
//    }
//
//    public Path errorDir() {
//        Path dir = resolvePath(definition.errorDir(), "data" + separator + "error" + separator + definition.sourceId());
//        try {
//            Files.createDirectories(dir);
//        } catch (IOException e) {
//            LOGGER.log(Level.WARNING, "Could not create error directory: " + dir, e);
//        }
//        return dir;
//    }
//
//    public Path outputDir() {
//        Path dir = resolvePath(definition.outputDir(), "data" + separator + "output" + separator + definition.sourceId());
//        try {
//            Files.createDirectories(dir);
//        } catch (IOException e) {
//            LOGGER.log(Level.WARNING, "Could not create output directory: " + dir, e);
//        }
//        return dir;
//    }
//
//    // These might be global or defined elsewhere. For now, assuming they could be relative.
//    public Path fileInfo() {
//        // Example: if definition had a field for it, otherwise global relative to pipeline config
//        return pipelineConfigBasePath.resolve("status" + separator + "file_info.csv");
//    }
//
//    public Path routeInfo() {
//        return pipelineConfigBasePath.resolve("status" + separator + "route_info.csv");
//    }
//
//    public String fileFilter() {
//        return definition.fileFilter() != null ? definition.fileFilter() : "*.*";
//    }
//
//    public String dirFilter() {
//        return definition.dirFilter(); // Can be null
//    }
//
//    public boolean isDirAsPartition() {
//        return definition.isDirAsPartition();
//    }
//
//    public boolean isActive() {
//        return definition.isActive();
//    }
//
//    public boolean isOutFileHeader() {
//        return definition.isOutFileHeader();
//    }
//
//    // These would ideally come from pipeline-level config, with source-level overrides
//    public int maxConcurrency() {
//        return definition.numThreads() != null ? definition.numThreads() : 1; // Default to 1 if not in source YAML
//    }
//
//    public int batchSize() {
//        return definition.batchSize() != null ? definition.batchSize() : 100; // Default if not in source YAML
//    }
//
//    public int headerLines() {
//        return definition.getHeaderLinesToSkip();
//    }
//
//    public int srcFieldSize() {
//        return this.dataRecordSchema.getFields().size();
//    }
//
//    public String getMeasures() {
//        return definition.matrixMeasures();
//    }
//
//    public String getDimensions() {
//        return definition.matrixDims();
//    }
//
//    public String getOptions() {
//        return definition.processOption();
//    }
//
//    public List<String> getFieldsNames(String nameType) {
//        if ("source".equalsIgnoreCase(nameType)) {
//            return this.dataRecordSchema.getFields().stream().map(Schema.Field::name).toList();
//        } else if ("TX".equalsIgnoreCase(nameType)) {
//            List<String> enriched = definition.enrichedFields();
//            return enriched != null ? new ArrayList<>(enriched) : Collections.emptyList();
//        }
//        LOGGER.log(Level.WARNING, "Unknown field name type requested: {0} for source {1}", new Object[]{nameType, sourceID()});
//        return Collections.emptyList();
//    }
//
//    public String getMatrixTableName() {
//        return definition.matrixTable();
//    }
//
//    public boolean isMatrixLoad() {
//        return definition.isMatrixLoad();
//    }
//}