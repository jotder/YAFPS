package org.gamma.config;

// AdjustableParameterDetailConfig is expected to be in the same package
// or imported if moved to its own file later.

public record AdjustableParametersConfig(
    AdjustableParameterDetailConfig numThreads,
    AdjustableParameterDetailConfig batchSize
) {
}
