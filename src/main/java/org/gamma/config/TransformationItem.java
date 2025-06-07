package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record TransformationItem(
    String transformationName,
    String type,
    String expression
) {
}
