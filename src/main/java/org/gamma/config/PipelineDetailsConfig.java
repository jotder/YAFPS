package org.gamma.config;

import java.util.List;

// SourceDetailsItem and OutputItem are expected to be in the same package
// or imported if they are moved to individual files later.

public record PipelineDetailsConfig(
    String pipelineName,
    List<SourceDetailsItem> sourcesDetails,
    List<OutputItem> outputs
) {
}
