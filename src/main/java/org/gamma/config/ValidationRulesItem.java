package org.gamma.config;

import java.util.List;

// Record for Validation Rules container (Unchanged structure, used in SourceDetailsItem)
public record ValidationRulesItem(Integer maxRecordLength, Integer defaultMaxFieldSize,
                                  List<FieldValidationConfigItem> fieldValidations) {
}
