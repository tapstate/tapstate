package io.tapstate.core.validation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationValueComparisonTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(65536, 1000, 32);

    @Test void exactNumbersKeepLargeIntegersFractionalPartsAndScaleIndependentEquality() {
        equal(1, 1L);
        equal(new BigDecimal("1.00"), new BigDecimal("1.0"));
        equal(new BigInteger("18446744073709551616"), new BigDecimal("18446744073709551616.000"));
        different(9007199254740992L, 9007199254740993L, ValidationValueComparison.Kind.VALUE);
        different(new BigDecimal("1.5"), 1L, ValidationValueComparison.Kind.VALUE);
        different(new BigDecimal("1E-1000000"), BigDecimal.ZERO, ValidationValueComparison.Kind.VALUE);
    }

    @Test void nativeNamesAndCodecBindingsDoNotBecomeValueEquality() {
        equal(new ValidationValues.Typed("exact-number", "NUMERIC(38,2)", "pg-artifact", new BigDecimal("1.00")),
                new ValidationValues.Typed("exact-number", "DECIMAL(20,2)", "mysql-artifact", 1L));
        different(new ValidationValues.Typed("exact-number", "NUMERIC", "binding", 1),
                new ValidationValues.Typed("exact-number", "NUMERIC", "binding", 2), ValidationValueComparison.Kind.VALUE);
    }

    @Test void logicalAliasesNeedExplicitRulesAndDoNotPermitTextNumberCoercion() {
        var source = new ValidationValues.Typed("identifier-a", "A", "source", "001");
        var target = new ValidationValues.Typed("identifier-b", "B", "target", "001");
        different(source, target, ValidationValueComparison.Kind.LOGICAL_TYPE);
        assertThat(ValidationValueComparison.compare(row(source), row(target),
                (left, right) -> left.equals("identifier-a") && right.equals("identifier-b"))).isEmpty();
        assertThat(ValidationValueComparison.compare(row("001"), row(1), (left, right) -> true))
                .get().extracting(ValidationValueComparison.Difference::kind).isEqualTo(ValidationValueComparison.Kind.LOGICAL_TYPE);
    }

    @Test void specialIdentifiersDoNotMatchLookalikeBusinessStringsByDefault() {
        String hex = "507f1f77bcf86cd799439011";
        different(new ValidationValues.Typed("object-id", "OBJECT_ID", "mongo", hex), hex,
                ValidationValueComparison.Kind.LOGICAL_TYPE);
        different("001", 1, ValidationValueComparison.Kind.LOGICAL_TYPE);
        different(true, 1, ValidationValueComparison.Kind.LOGICAL_TYPE);
        different("abc ", "abc", ValidationValueComparison.Kind.VALUE);
        different("ABC", "abc", ValidationValueComparison.Kind.VALUE);
    }

    @Test void floatingPointHasNoImplicitToleranceAndPreservesSpecialValueRules() {
        equal(-0.0, 0.0);
        equal(-0.0f, 0.0f);
        equal(Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L));
        equal(Float.NaN, Float.intBitsToFloat(0x7fc00001));
        equal(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
        different(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, ValidationValueComparison.Kind.VALUE);
        different(Double.NaN, 0.0, ValidationValueComparison.Kind.VALUE);
        different(1.0, Math.nextUp(1.0), ValidationValueComparison.Kind.VALUE);
        different(1.0f, Math.nextUp(1.0f), ValidationValueComparison.Kind.VALUE);
    }

    @Test void declaredFloatWidthMappingWidensExactlyRatherThanRoundingToDecimalText() {
        different(0.5f, 0.5d, ValidationValueComparison.Kind.LOGICAL_TYPE);
        BiPredicate<String, String> widths = (a, b) -> a.equals("float32") && b.equals("float64");
        assertThat(ValidationValueComparison.compare(row(0.5f), row(0.5d), widths)).isEmpty();
        assertThat(ValidationValueComparison.compare(row(0.1f), row(0.1d), widths)).isPresent();
        assertThat(ValidationValueComparison.compare(row(Float.NaN), row(Double.NaN), widths)).isEmpty();
    }

    @Test void explicitExactFloatMappingStillRequiresMathematicallyExactValues() {
        different(new BigDecimal("0.5"), 0.5d, ValidationValueComparison.Kind.LOGICAL_TYPE);
        BiPredicate<String, String> exactFloat = (a, b) -> a.equals("exact-number") && b.equals("float64");
        assertThat(ValidationValueComparison.compare(row(new BigDecimal("0.5")), row(0.5d), exactFloat)).isEmpty();
        assertThat(ValidationValueComparison.compare(row(new BigDecimal("0.1")), row(0.1d), exactFloat)).isPresent();
        assertThat(ValidationValueComparison.compare(row(new BigDecimal("9007199254740993")),
                row(9007199254740992d), exactFloat)).isPresent();
        assertThat(ValidationValueComparison.compare(row(BigDecimal.ZERO), row(Double.NaN), exactFloat)).isPresent();
    }

    @Test void dateTimeFamiliesRemainSeparateAndInstantsKeepNanoseconds() {
        equal(Instant.parse("2026-10-21T00:00:00.123456789Z"),
                OffsetDateTime.parse("2026-10-21T08:00:00.123456789+08:00").toInstant());
        different(Instant.parse("2026-10-21T00:00:00.123456789Z"),
                Instant.parse("2026-10-21T00:00:00.123456000Z"), ValidationValueComparison.Kind.VALUE);
        equal(LocalDate.of(2026, 10, 21), LocalDate.of(2026, 10, 21));
        different(LocalTime.of(1, 2, 3, 1), LocalTime.of(1, 2, 3, 2), ValidationValueComparison.Kind.VALUE);
        var local = LocalDateTime.of(2026, 10, 21, 0, 0);
        var instant = Instant.parse("2026-10-21T00:00:00Z");
        different(local, instant, ValidationValueComparison.Kind.LOGICAL_TYPE);
        assertThat(ValidationValueComparison.compare(row(local), row(instant), (a, b) -> true))
                .get().extracting(ValidationValueComparison.Difference::kind).isEqualTo(ValidationValueComparison.Kind.LOGICAL_TYPE);
    }

    @Test void binaryContentAndSubtypeMatterIndependentlyOfObjectIdentity() {
        equal(new ValidationValues.Binary(4, new byte[]{1, 2}), new ValidationValues.Binary(4, new byte[]{1, 2}));
        different(new ValidationValues.Binary(4, new byte[]{1, 2}), new ValidationValues.Binary(4, new byte[]{1, 3}),
                ValidationValueComparison.Kind.VALUE);
        different(new ValidationValues.Binary(4, new byte[]{1, 2}), new ValidationValues.Binary(3, new byte[]{1, 2}),
                ValidationValueComparison.Kind.VALUE);
    }

    @Test void nullMissingAndEmptyShapesRemainDistinctAtEveryDepth() {
        equal(null, null);
        equal(new ValidationValues.Typed("text", "varchar", "source", null), null);
        different(null, "", ValidationValueComparison.Kind.VALUE);
        different(Map.of(), List.of(), ValidationValueComparison.Kind.LOGICAL_TYPE);
        var left = new LinkedHashMap<String, Object>();
        left.put("present", null);
        var missing = ValidationValueComparison.compare(ValidationValues.ownRow(left, LIMITS),
                ValidationValues.ownRow(Map.of(), LIMITS)).orElseThrow();
        assertThat(missing.kind()).isEqualTo(ValidationValueComparison.Kind.MISSING_TARGET);
        assertThat(missing.path()).containsExactly("present");
        var extra = ValidationValueComparison.compare(ValidationValues.ownRow(Map.of(), LIMITS),
                ValidationValues.ownRow(left, LIMITS)).orElseThrow();
        assertThat(extra.kind()).isEqualTo(ValidationValueComparison.Kind.MISSING_SOURCE);
    }

    @Test void objectFieldOrderIsIgnoredButLiteralNamesAndNestedTypesArePreserved() {
        var left = new LinkedHashMap<String, Object>();
        left.put("a", new BigDecimal("1.00")); left.put("b", true);
        var right = new LinkedHashMap<String, Object>();
        right.put("b", true); right.put("a", 1L);
        equal(left, right);
        var mismatch = ValidationValueComparison.compare(
                row(List.of(Map.of("literal.dot", new ValidationValues.Typed("object-id", null, "source", "x")))),
                row(List.of(Map.of("literal.dot", "x")))).orElseThrow();
        assertThat(mismatch.kind()).isEqualTo(ValidationValueComparison.Kind.LOGICAL_TYPE);
        assertThat(mismatch.path()).containsExactly("value", 0, "literal.dot");
        assertThatThrownBy(() -> mismatch.path().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void ordinaryArraysKeepOrderAndOccurrenceCountsWithoutUsingNestRules() {
        equal(Arrays.asList(1, null, 2), Arrays.asList(1L, null, new BigDecimal("2.0")));
        var reordered = ValidationValueComparison.compare(row(List.of(1, 2)), row(List.of(2, 1))).orElseThrow();
        assertThat(reordered.kind()).isEqualTo(ValidationValueComparison.Kind.VALUE);
        assertThat(reordered.path()).containsExactly("value", 0);
        var repeated = ValidationValueComparison.compare(row(List.of(1, 1)), row(List.of(1))).orElseThrow();
        assertThat(repeated.kind()).isEqualTo(ValidationValueComparison.Kind.MISSING_TARGET);
        assertThat(repeated.path()).containsExactly("value", 1);
    }

    private static ValidationValues.Row row(Object value) {
        var row = new LinkedHashMap<String, Object>();
        row.put("value", value);
        return ValidationValues.ownRow(row, LIMITS);
    }

    private static void equal(Object source, Object target) {
        assertThat(ValidationValueComparison.compare(row(source), row(target))).isEmpty();
    }

    private static void different(Object source, Object target, ValidationValueComparison.Kind kind) {
        assertThat(ValidationValueComparison.compare(row(source), row(target)))
                .get().extracting(ValidationValueComparison.Difference::kind).isEqualTo(kind);
    }
}
