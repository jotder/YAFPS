package org.gamma.config;

import java.nio.file.Path;

// MODIFIED: Record for a Source Configuration (core settings in main framework file)
public record SourceItem(String sourceId, boolean active, Path sourceDir, String dirFilter, String startDir, String endDir,
                  String dataFileType, String fileFilter, Boolean fileBackup, Boolean deleteAfterLoad,
                  // From source dir before staging DB load
                  Path backupDir, Boolean errBackup, Path errDir, Path outDir, // For intermediate files if any
                  Boolean useSubDirAsPartition, Integer headerLines, Integer numThreads, // Initial number of threads
                  Integer batchSize, Boolean loadFiles,  // Load raw files to a staging table
                  String databaseConnectionRef, String tableName, // Staging table name
                  Boolean keepFileHeader, Boolean deleteFilesOnLoadAfterDb,
                  // From source dir after successful staging DB load
                  String jsonPathExpression // Optional, for JSON type
                  // MOVED to PipelineDetailsConfig/SourceDetailsItem: sourceMetadata, sourceFields, validationRules
) {
}
