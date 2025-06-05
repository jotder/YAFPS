package org.gamma.config;

import java.util.List;

// Top-level Configuration Record for the main framework file
public record AppConfig(List<DatabaseConnectionItem> databaseConnections, List<EtlPipelineItem> etlPipelines) {
}
