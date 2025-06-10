package org.gamma.config;

public record RuleActionConfig(
        String type,
        Integer durationMinutes,
        RuleActionParamsConfig params,
        RuleActionConfig interimAction, // Self-referential type
        String reason
) {
}
