package org.gamma.config;

// No special imports needed for this record beyond standard Java types.

public record ValidationExpressionItem(
    String expression,
    String errorMessage,
    String actionOnFailure
) {
}
