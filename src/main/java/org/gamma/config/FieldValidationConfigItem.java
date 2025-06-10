package org.gamma.config;

import java.util.List;

public record FieldValidationConfigItem(
        String fieldName,
        List<ValidationExpressionItem> validations
) {
}
