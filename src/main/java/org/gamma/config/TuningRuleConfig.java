package org.gamma.config;

import java.util.List;

// SingleConditionConfig and RuleActionConfig are expected to be in the same package
// or imported if they are moved to individual files later.

public record TuningRuleConfig(
    String ruleName,
    String metric,
    String condition,
    Double threshold,
    String conditionLogic,
    List<SingleConditionConfig> conditions,
    Integer consecutiveBreachesToTrigger,
    RuleActionConfig action
) {
}
