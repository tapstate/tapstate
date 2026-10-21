package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationValueComparison.*;
import static org.assertj.core.api.Assertions.*;

class ValidationValueTraversalTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(64 * 1024, 1024, 32);
    private static final TraversalLimits LIMITS = new TraversalLimits(ROW, 100000);

    @Test void visitsIndependentDifferencesAfterTheFirstWithoutAccumulateInsideTheComparator() {
        var left = row(ordered("a", 1, "b", 2, "nested", Map.of("x", 3), "array", List.of(1, 2, 3)));
        var right = row(ordered("a", 9, "b", 8, "nested", Map.of("x", 4), "array", List.of(1), "extra", 0));
        var found = new ArrayList<Difference>();
        assertThat(visitDifferences(left, right, String::equals, LIMITS, found::add)).isEqualTo(6);
        assertThat(found).containsExactly(new Difference(Kind.VALUE, List.of("a")), new Difference(Kind.VALUE, List.of("b")),
                new Difference(Kind.VALUE, List.of("nested", "x")), new Difference(Kind.MISSING_TARGET, List.of("array", 1)),
                new Difference(Kind.MISSING_TARGET, List.of("array", 2)), new Difference(Kind.MISSING_SOURCE, List.of("extra")));
        assertThat(compare(left, right)).contains(found.getFirst());
    }

    @Test void droppingDiagnosticSamplesDoesNotStopRemainingNecessaryChecks() {
        var left = row(Map.of("items", List.of(1, 2, 3, 4, 5)));
        var right = row(Map.of("items", List.of(9, 9, 9, 9, 9)));
        var samples = new ArrayList<Difference>();
        long count = visitDifferences(left, right, String::equals, LIMITS, difference -> {
            if (samples.size() < 1) samples.add(difference);
        });
        assertThat(count).isEqualTo(5);
        assertThat(samples).containsExactly(new Difference(Kind.VALUE, List.of("items", 0)));
    }

    @Test void missingAndIncompatibleParentsAreSingleConclusionsWithoutInventingSubtreeComparisons() {
        var left = row(ordered("missing", Map.of("a", 1, "b", 2), "shape", List.of(1, 2), "other", 7));
        var right = row(Map.of("shape", "text", "other", 8));
        var found = new ArrayList<Difference>();
        assertThat(visitDifferences(left, right, String::equals, LIMITS, found::add)).isEqualTo(3);
        assertThat(found).containsExactly(new Difference(Kind.MISSING_TARGET, List.of("missing")),
                new Difference(Kind.LOGICAL_TYPE, List.of("shape")), new Difference(Kind.VALUE, List.of("other")));
    }

    @Test void nullMissingTypedNumbersAndOrdinaryArrayOrderingRetainExistingRules() {
        var left = row(ordered("null", null, "number", new ValidationValues.Typed("exact-number", "decimal", "source", new BigDecimal("123.00")),
                "items", List.of(1, 2), "present", null));
        var right = row(ordered("null", new ValidationValues.Typed("text", null, "target", null), "number", 123L, "items", List.of(2, 1)));
        var found = new ArrayList<Difference>();
        assertThat(visitDifferences(left, right, String::equals, LIMITS, found::add)).isEqualTo(3);
        assertThat(found).containsExactly(new Difference(Kind.VALUE, List.of("items", 0)),
                new Difference(Kind.VALUE, List.of("items", 1)), new Difference(Kind.MISSING_TARGET, List.of("present")));
    }

    @Test void exactCrossDomainNumericsAndFirstDifferenceAgreeWithTheExistingEntryPoint() {
        var values = List.of(1L, new BigDecimal("1.000"), 0.1d, new BigDecimal("0.1"), Float.NaN, Double.NaN,
                -0.0d, 0.0d, Double.POSITIVE_INFINITY, "x", false,
                new ValidationValues.Binary(1, new byte[] {1}), new ValidationValues.Binary(2, new byte[] {1}));
        for (Object source : values) {
            for (Object target : values) {
                var left = row(Map.of("value", source));
                var right = row(Map.of("value", target));
                var found = new ArrayList<Difference>();
                var first = compare(left, right, (a, b) -> true);
                long count = visitDifferences(left, right, (a, b) -> true, LIMITS, found::add);
                assertThat(found.stream().findFirst()).isEqualTo(first);
                assertThat(count).isEqualTo(first.isPresent() ? 1 : 0);
            }
        }
    }

    @Test void callbackFailureAndFatalErrorsEscapeWithoutReturningACompletedCount() {
        var left = row(ordered("a", 1, "b", 2));
        var right = row(Map.of("a", 8, "b", 9));
        var failure = new IllegalStateException("callback failed");
        assertThat(catchThrowable(() -> visitDifferences(left, right, String::equals, LIMITS, difference -> { throw failure; })))
                .isSameAs(failure);
        var fatal = new AssertionError("fatal callback");
        assertThat(catchThrowable(() -> visitDifferences(left, right, String::equals, LIMITS, difference -> { throw fatal; })))
                .isSameAs(fatal);
    }

    @Test void operationAndRowBudgetFailuresDoNotBecomeEqualOrACompletedPartialResult() {
        var left = row(Map.of("value", "x".repeat(1000)));
        var right = row(Map.of("value", "x".repeat(1000)));
        assertThatThrownBy(() -> visitDifferences(left, right, String::equals, new TraversalLimits(ROW, 100), ignored -> {}))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThatThrownBy(() -> visitDifferences(left, right, String::equals,
                new TraversalLimits(new ValidationValues.Limits(1, 1, 1), 100000), ignored -> {})).isInstanceOf(TapstateException.class);
        var firstObserved = new ArrayList<Difference>();
        assertThatThrownBy(() -> visitDifferences(row(ordered("first", 1, "large", "x".repeat(1000))),
                row(Map.of("first", 2, "large", "x".repeat(1000))), String::equals,
                new TraversalLimits(ROW, 300), firstObserved::add)).isInstanceOf(TapstateException.class);
        assertThat(firstObserved).containsExactly(new Difference(Kind.VALUE, List.of("first")));
    }

    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static Map<String, Object> ordered(Object... values) {
        var result = new LinkedHashMap<String, Object>();
        for (int i = 0; i < values.length; i += 2) result.put((String) values[i], values[i + 1]);
        return result;
    }
}
