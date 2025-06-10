package org.gamma.config;

public record AdjustableParameterDetailConfig(Integer min, Integer max, Integer incrementStep, Integer decrementStep,
                                              Double incrementFactor,
                                              Double decrementFactor) {
}
