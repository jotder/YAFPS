package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record RuleActionParamsConfig(
    Integer numThreadsIncrement,
    Integer numThreadsDecrement,
    String targetNumThreads,
    Double batchSizeIncrementFactor,
    Double batchSizeDecrementFactor,
    Double targetBatchSizeFactor
) {
}
