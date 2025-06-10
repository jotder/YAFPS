package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record SingleConditionConfig(
        String metric,
        String condition,
        Double threshold
) {
}
