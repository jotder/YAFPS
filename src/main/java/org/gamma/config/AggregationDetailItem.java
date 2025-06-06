package org.gamma.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AggregationDetailItem(
    @JsonProperty("input_field") String inputField,
    String function
) {
}
