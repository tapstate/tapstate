package io.tapstate.core.validation;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Set;

/** Diagnosable refusals at independent validation boundaries. */
public enum ValidationError implements TapstateErrorCode {
    EXECUTION_CANCELLED("validation.execution-cancelled", Set.of("node")),
    SOURCE_FIELD_CONFLICT("validation.source-field-conflict", Set.of("node", "field")),
    RULE_UNSUPPORTED("validation.rule-unsupported", Set.of("node", "field", "rule")),
    INVALID_CONTRACT("validation.invalid-contract", Set.of("field")),
    KEY_LIMIT_EXCEEDED("validation.key-limit-exceeded", Set.of("limit")),
    INCOMPATIBLE_KEY("validation.incompatible-key", Set.of("component")),
    ILLEGAL_TRANSITION("validation.illegal-transition", Set.of("from", "to", "mode")),
    VALUE_LIMIT_EXCEEDED("validation.value-limit-exceeded", Set.of("limit")),
    VALUE_TYPE_UNSUPPORTED("validation.value-type-unsupported", Set.of("type")),
    OWNER_NOT_AUTHORIZED("validation.owner-not-authorized", Set.of("pipeline")),
    READ_FAILED("validation.read-failed", Set.of("table")),
    READ_PROTOCOL_VIOLATION("validation.read-protocol-violation", Set.of("table")),
    READ_CANCELLED("validation.read-cancelled", Set.of("table")),
    CODEC_FAILED("validation.codec-failed", Set.of("connector", "field")),
    CODEC_VALUE_UNAVAILABLE("validation.codec-value-unavailable", Set.of("connector", "field"));

    private final String code;
    private final Set<String> placeholders;

    ValidationError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override public String code() { return code; }
    @Override public Severity severity() { return Severity.ERROR; }
    @Override public Set<String> placeholders() { return placeholders; }

    static TapstateException invalid(String field) {
        return new TapstateException(INVALID_CONTRACT, Map.of("field", field), null);
    }

    static String identifier(String value, String field) {
        if (value == null || value.isBlank() || value.length() > 512) {
            throw invalid(field);
        }
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
