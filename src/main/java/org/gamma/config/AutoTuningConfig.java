package org.gamma.config;

import java.util.List;

public record AutoTuningConfig(Boolean enabled, Integer observationFrequencySeconds, Integer cooldownPeriodSeconds,
                               AdjustableParametersConfig adjustableParameters, List<TuningRuleConfig> tuningRules) {
}
