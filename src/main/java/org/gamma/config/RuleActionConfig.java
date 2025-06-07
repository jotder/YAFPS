package org.gamma.config;

// RuleActionParamsConfig is expected to be in the same package
// or imported if moved to its own file later.

public record RuleActionConfig(
    String type,
    Integer durationMinutes,
    RuleActionParamsConfig params,
    RuleActionConfig interimAction, // Self-referential type
    String reason
) {
}
