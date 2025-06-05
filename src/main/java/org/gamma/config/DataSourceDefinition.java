package org.gamma.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Collections;

// This record maps to the 'source' item in your pipeline YAML
public record DataSourceDefinition(
        String sourceId,
        String sourceName,
        Boolean active,
        String avroSchemaPath, // Path to the Avro schema file
        String sourceDir,
        String errorDir,
        String outputDir,
        String fileFilter,
        String dirFilter, // Optional
        @JsonProperty("headerLinesToSkip") Integer headerLinesToSkip, // Maps to headerLines in SourceConfig
        Boolean dirAsPartition,
        Boolean outFileHeader,
        String matrixMeasures,
        String matrixDims,
        String processOption,
        String matrixTable,
        Boolean matrixLoad,
        List<String> enrichedFields,
        // Optional overrides for pipeline-level settings
        Integer numThreads,
        Integer batchSize
) {
    // Constructor with defaults for cleaner instantiation or handling missing YAML fields
    public DataSourceDefinition {
        if (active == null) active = true; // Default to active
        if (headerLinesToSkip == null) headerLinesToSkip = 1;
        if (dirAsPartition == null) dirAsPartition = false;
        if (outFileHeader == null) outFileHeader = true;
        if (enrichedFields == null) enrichedFields = Collections.emptyList();
    }

    // Convenience getters with defaults if needed, matching old SourceConfig methods
    public boolean isActive() { return active != null && active; }
    public int getHeaderLinesToSkip() { return headerLinesToSkip == null ? 1 : headerLinesToSkip; }
    public boolean isDirAsPartition() { return dirAsPartition != null && dirAsPartition; }
    public boolean isOutFileHeader() { return outFileHeader != null && outFileHeader; }
    public boolean isMatrixLoad() { return matrixLoad != null && matrixLoad; }
}