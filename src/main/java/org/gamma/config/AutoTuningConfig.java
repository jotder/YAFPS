package org.gamma.config;

import java.util.List;

// --- Records for Performance Tuning (Unchanged structures, part of EtlPipelineItem) ---
public record AutoTuningConfig(Boolean enabled, Integer observationFrequencySeconds, Integer cooldownPeriodSeconds,
                        AdjustableParametersConfig adjustableParameters, List<TuningRuleConfig> tuningRules) {
}
