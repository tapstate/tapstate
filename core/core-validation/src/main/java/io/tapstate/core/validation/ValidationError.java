package io.tapstate.core.validation;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ValidationErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Set;

/** Diagnosable refusals at independent validation boundaries. */
public enum ValidationError implements TapstateErrorCode {
    SCHEDULE_FIELD_COUNT(ValidationErrorDefinitions.SCHEDULE_FIELD_COUNT),
    SCHEDULE_INVALID(ValidationErrorDefinitions.SCHEDULE_INVALID),
    SCHEDULE_NO_DATE(ValidationErrorDefinitions.SCHEDULE_NO_DATE),
    SCHEDULE_SEARCH_EXHAUSTED(ValidationErrorDefinitions.SCHEDULE_SEARCH_EXHAUSTED),
    EXECUTION_CANCELLED(ValidationErrorDefinitions.EXECUTION_CANCELLED),
    SOURCE_FIELD_CONFLICT(ValidationErrorDefinitions.SOURCE_FIELD_CONFLICT),
    RULE_UNSUPPORTED(ValidationErrorDefinitions.RULE_UNSUPPORTED),
    INVALID_CONTRACT(ValidationErrorDefinitions.INVALID_CONTRACT),
    KEY_LIMIT_EXCEEDED(ValidationErrorDefinitions.KEY_LIMIT_EXCEEDED),
    INCOMPATIBLE_KEY(ValidationErrorDefinitions.INCOMPATIBLE_KEY),
    ILLEGAL_TRANSITION(ValidationErrorDefinitions.ILLEGAL_TRANSITION),
    VALUE_LIMIT_EXCEEDED(ValidationErrorDefinitions.VALUE_LIMIT_EXCEEDED),
    VALUE_TYPE_UNSUPPORTED(ValidationErrorDefinitions.VALUE_TYPE_UNSUPPORTED),
    OWNER_NOT_AUTHORIZED(ValidationErrorDefinitions.OWNER_NOT_AUTHORIZED),
    READ_FAILED(ValidationErrorDefinitions.READ_FAILED),
    READ_PROTOCOL_VIOLATION(ValidationErrorDefinitions.READ_PROTOCOL_VIOLATION),
    READ_CANCELLED(ValidationErrorDefinitions.READ_CANCELLED),
    CODEC_FAILED(ValidationErrorDefinitions.CODEC_FAILED),
    CODEC_VALUE_UNAVAILABLE(ValidationErrorDefinitions.CODEC_VALUE_UNAVAILABLE);

    private final ProductErrorDefinition definition;

    ValidationError(ProductErrorDefinition definition) {
        this.definition = definition;
    }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }

    static TapstateException invalid(String field) {
        return new TapstateException(INVALID_CONTRACT, Map.of("field", field), null);
    }

    static String identifier(String value, String field) {
        if (value == null || value.isBlank()) throw invalid(field);
        return fieldName(value, field);
    }

    /** Validates a literal data field name, including empty and whitespace-only JSON members. */
    public static String fieldName(String value, String field) {
        if (value == null || value.length() > 512) throw invalid(field);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                    throw invalid(field);
                }
            } else if (Character.isLowSurrogate(c)) {
                throw invalid(field);
            }
        }
        return value;
    }

    static long positive(long value, String field) {
        if (value <= 0) throw invalid(field);
        return value;
    }
}
