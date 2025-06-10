package org.gamma.config;

public record RuleActionParamsConfig(
        Integer numThreadsIncrement,
        Integer numThreadsDecrement,
        String targetNumThreads,
        Double batchSizeIncrementFactor,
        Double batchSizeDecrementFactor,
        Double targetBatchSizeFactor
) {
}
