package org.gamma.config;

public record AdjustableParametersConfig(AdjustableParameterDetailConfig numThreads,
                                         AdjustableParameterDetailConfig batchSize) {
}
