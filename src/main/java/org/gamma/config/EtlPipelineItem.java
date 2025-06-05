package org.gamma.config;

import java.nio.file.Path;
import java.util.List;

// MODIFIED: Record for an ETL Pipeline (in main framework file)
public record EtlPipelineItem(String pipelineName, String description, String pipelineVersion, boolean active, Path logFile,
                              Path statusDir, String fileInfoTable, String routeInfoTable, Path pipelineSpecificConfigFile,
                              // Path to the detailed config for this pipeline
                              List<SourceItem> sources, // Core source settings
                              AutoTuningConfig autoTuning) {
}
