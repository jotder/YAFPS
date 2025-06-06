package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record AdjustableParameterDetailConfig(
    Integer min,
    Integer max,
    Integer incrementStep,
    Integer decrementStep,
    Double incrementFactor,
    Double decrementFactor
) {
}
