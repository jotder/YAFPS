package org.gamma.config;

public record SourceDetailsItem(
        String sourceId,
        String sourceMetadata,
        String sourceFields,
        ValidationRulesItem validationRules) {
}
