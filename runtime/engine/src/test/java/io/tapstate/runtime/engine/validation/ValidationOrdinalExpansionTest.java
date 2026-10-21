package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationOrdinalExpansionTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 150, 16);
    private static final ValidationKey PARENT = new ValidationKey(List.of(ValidationKey.Part.number(7)));

    @Test void outputsOneBorrowedRowAtATimeWithOriginalOrdinalsAndOwnedComparisonKeys() {
        var input = row(Map.of("id", 7L, "items", List.of("first", "second"), "keep", true));
        var expansion = compile("items", "position", LIMITS, 10);
        try (var cursor = ready(expansion.prepare(input, PARENT))) {
            try (var first = cursor.next().orElseThrow()) {
                assertThat(first.ordinal()).isZero();
                assertThat(first.comparisonKey()).isEqualTo(key(0));
                assertThat(first.values().fields()).containsEntry("items", "first").containsEntry("position", 0L).containsEntry("keep", true);
                assertThatThrownBy(cursor::next).isInstanceOf(IllegalStateException.class);
            }
            try (var second = cursor.next().orElseThrow()) {
                assertThat(second.ordinal()).isEqualTo(1);
                assertThat(second.comparisonKey()).isEqualTo(key(1));
                assertThat(second.values().fields()).containsEntry("items", "second").containsEntry("position", 1L);
            }
            assertThat(cursor.next()).isEmpty(); assertThat(cursor.next()).isEmpty();
        }
        assertThat(input.fields()).containsEntry("items", List.of("first", "second")).doesNotContainKey("position");
        assertThat(PARENT.parts()).hasSize(1);
        assertThat(expansion.workingSetBytes(input, PARENT)).isGreaterThan(input.accountedBytes());
    }

    @Test void missingNullAndEmptyArraysAreEmptyButNullMembersAreRealOrdinalRows() {
        var expansion = compile("items", "position", LIMITS, 10);
        for (var input : List.of(row(Map.of("id", 7L)), withItems(null), withItems(List.of()),
                withItems(new ValidationValues.Typed("array", "array", "codec", null)))) {
            try (var cursor = ready(expansion.prepare(input, PARENT))) { assertThat(cursor.next()).isEmpty(); }
        }
        try (var cursor = ready(expansion.prepare(withItems(Arrays.asList(null, "next")), PARENT))) {
            try (var first = cursor.next().orElseThrow()) {
                assertThat(first.ordinal()).isZero(); assertThat(first.values().fields()).containsEntry("items", null);
            }
            try (var second = cursor.next().orElseThrow()) { assertThat(second.ordinal()).isEqualTo(1); }
            assertThat(cursor.next()).isEmpty();
        }
    }

    @Test void nonArraysRemainOneOriginalScalarOrObjectIncludingEmptyValues() {
        var typed = new ValidationValues.Typed("exact-number", "decimal", "codec", new BigDecimal("1.00"));
        for (Object scalar : List.of("", Map.of(), false, 3L, typed)) {
            try (var cursor = ready(compile("items", "position", LIMITS, 10).prepare(withItems(scalar), PARENT))) {
                try (var output = cursor.next().orElseThrow()) {
                    assertThat(output.ordinal()).isZero(); assertThat(output.values().fields()).containsEntry("items", scalar);
                }
                assertThat(cursor.next()).isEmpty();
            }
        }
    }

    @Test void pathsAreLiteralAndTypedArrayMembersKeepTheirOwnProvenance() {
        var member = new ValidationValues.Typed("text", "varchar", "codec", "custom", "hello");
        var input = row(Map.of("a.b", new ValidationValues.Typed("array", "list", "codec", List.of(member)), "a", Map.of("b", "untouched")));
        try (var cursor = ready(compile("a.b", "position", LIMITS, 10).prepare(input, PARENT))) {
            try (var output = cursor.next().orElseThrow()) {
                assertThat(output.values().fields()).containsEntry("a.b", member).containsEntry("a", Map.of("b", "untouched"));
            }
            assertThat(cursor.next()).isEmpty();
        }
    }

    @Test void anExistingOrdinalColumnIsAnExplicitParentConflictEvenWithoutAnArray() {
        var input = row(Map.of("id", 7L, "position", "original"));
        var result = compile("items", "position", LIMITS, 10).prepare(input, PARENT);
        assertThat(result).isInstanceOfSatisfying(ValidationOrdinalExpansion.Conflict.class, conflict -> {
            assertThat(conflict.parentKey()).isEqualTo(PARENT);
            assertThat(conflict.code()).isEqualTo(ValidationError.SOURCE_FIELD_CONFLICT);
            assertThat(conflict.args()).containsEntry("node", "expand").containsEntry("field", "position");
        });
        assertThat(input.fields()).containsEntry("position", "original");
    }

    @Test void unsupportedDeclarationsAndContainerContradictionsNeverCreateAReadyCursor() {
        for (var declaration : List.of(new TransformBody.Unwind("items", null, false, null, null),
                new TransformBody.Unwind("items", "position", true, null, null),
                new TransformBody.Unwind("items", "position", false, "id", null),
                new TransformBody.Unwind("items", "items", false, null, null))) {
            assertThatThrownBy(() -> ValidationOrdinalExpansion.compile("expand", declaration, LIMITS, 10))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
        for (Object invalid : List.of(new ValidationValues.Typed("text", "wrong", "codec", List.of(1)),
                new ValidationValues.Typed("array", "wrong", "codec", "not-an-array"))) {
            assertThatThrownBy(() -> compile("items", "position", LIMITS, 10).prepare(withItems(invalid), PARENT))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
    }

    @Test void elementAndDerivedKeyLimitsAreCheckedBeforeTheFirstOutput() {
        assertThatThrownBy(() -> compile("items", "position", LIMITS, 1).prepare(withItems(List.of(1, 2)), PARENT))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "unwind.elements"));
        var tooWide = new ValidationKey(java.util.Collections.nCopies(32, ValidationKey.Part.number(1)));
        assertThatThrownBy(() -> compile("items", "position", LIMITS, 10).prepare(withItems(List.of()), tooWide))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
    }

    @Test void invalidGeneratedColumnNamesAreRefusedBeforeAnyInputIsRead() {
        assertThatThrownBy(() -> compile("items", String.valueOf((char)0xD800), LIMITS, 10))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }

    @Test void earlyCloseIsCancellationNotEofAndInvalidatesTheOutstandingOutput() {
        var cursor = ready(compile("items", "position", LIMITS, 10).prepare(withItems(List.of(1, 2)), PARENT));
        var output = cursor.next().orElseThrow(); cursor.close();
        assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.EXECUTION_CANCELLED));
        assertThatThrownBy(output::values).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.EXECUTION_CANCELLED));
        cursor.close(); output.close();
    }

    @Test void outputBudgetFailureCannotBecomeEofOrBeReplacedByCleanupCancellation() {
        var input = withItems("x".repeat(500));
        var small = new ValidationValues.Limits(input.accountedBytes(), 150, 16);
        var cursor = ready(compile("items", "position", small, 10).prepare(input, PARENT));
        var first = catchThrowable(cursor::next);
        assertThat(first).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        cursor.close();
        assertThat(catchThrowable(cursor::next)).isSameAs(first);
    }

    private static ValidationOrdinalExpansion compile(String path, String ordinal, ValidationValues.Limits limits, int elements) {
        return ValidationOrdinalExpansion.compile("expand", new TransformBody.Unwind(path, ordinal, false, null, null), limits, elements);
    }
    private static ValidationOrdinalExpansion.Cursor ready(ValidationOrdinalExpansion.Preparation prepared) {
        return ((ValidationOrdinalExpansion.Ready)prepared).cursor();
    }
    private static ValidationKey key(long ordinal) {
        return new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.number(ordinal)));
    }
    private static ValidationValues.Row withItems(Object items) {
        var fields = new LinkedHashMap<String, Object>(); fields.put("id", 7L); fields.put("items", items); return row(fields);
    }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, LIMITS); }
}
