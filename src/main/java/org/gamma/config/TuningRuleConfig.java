package org.gamma.config;

import java.util.List;

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
