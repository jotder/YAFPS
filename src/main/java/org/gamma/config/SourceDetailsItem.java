package org.gamma.config;

// ValidationRulesItem is expected to be in the same package
// or imported if moved to its own file later.

public record SourceDetailsItem(
    String sourceId,
    String sourceMetadata,
    String sourceFields,
    ValidationRulesItem validationRules
) {
}
