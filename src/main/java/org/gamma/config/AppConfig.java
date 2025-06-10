package org.gamma.config;

import java.util.List;

public record AppConfig(List<DatabaseConnectionItem> databaseConnections, List<EtlPipelineItem> etlPipelines) {
}
