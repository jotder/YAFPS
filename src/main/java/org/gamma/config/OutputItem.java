package org.gamma.config;

import java.util.List;
import java.util.Map;

// Record for an Output Configuration (Unchanged structure, now part of PipelineDetailsConfig)
public record OutputItem(String outputId, String outputType, boolean active, List<TransformationItem> transformations,
                         Boolean deleteIntermediateFilesOnLoad, Boolean loadToDatabase, Boolean keepFileHeaderOnLoad,
                         String databaseConnectionRef, String tableName, String matrixName, String measures, String dimensions,
                         String groupByFields, Map<String, AggregationDetailItem> aggregations, String filterExpression,
                         List<String> exportFields, String exportFormat, String exportDirectory, String fileNamePattern) {
}
