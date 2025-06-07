package org.gamma.config;

import java.util.List;

// ValidationExpressionItem is expected to be in the same package
// or imported if moved to its own file later.

public record FieldValidationConfigItem(
    String fieldName,
    List<ValidationExpressionItem> validations
) {
}
