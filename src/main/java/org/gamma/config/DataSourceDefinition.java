package org.gamma.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public record DataSourceDefinition(String sourceId, String sourceName, Boolean active, String avroSchemaPath,
                                   String sourceDir, String errorDir, String outputDir, String fileFilter,
                                   String dirFilter, @JsonProperty("headerLinesToSkip") Integer headerLinesToSkip,
                                   Boolean dirAsPartition, Boolean outFileHeader, String matrixMeasures,
                                   String matrixDims, String processOption, String matrixTable, Boolean matrixLoad,
                                   List<String> enrichedFields, Integer numThreads, Integer batchSize) {
    public DataSourceDefinition {
        if (active == null) active = true; // Default to active
        if (headerLinesToSkip == null) headerLinesToSkip = 1;
        if (dirAsPartition == null) dirAsPartition = false;
        if (outFileHeader == null) outFileHeader = true;
        if (enrichedFields == null) enrichedFields = Collections.emptyList();
    }

    public boolean isActive() {
        return active != null && active;
    }

    public int getHeaderLinesToSkip() {
        return headerLinesToSkip == null ? 1 : headerLinesToSkip;
    }

    public boolean isDirAsPartition() {
        return dirAsPartition != null && dirAsPartition;
    }

    public boolean isOutFileHeader() {
        return outFileHeader != null && outFileHeader;
    }

    public boolean isMatrixLoad() {
        return matrixLoad != null && matrixLoad;
    }
}