package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static io.tapstate.core.validation.ValidationKey.Kind.*;
import static io.tapstate.core.validation.ValidationKeyBinding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidationKeyBindingTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(1024 * 1024, 1024, 32);

    @Test void explicitlyRenamedPhysicalFieldsProduceTheSameOrderedLogicalKeyWithoutNarrowing() {
        var source = binding(component("tenant", TEXT), component("id", EXACT_NUMBER));
        var target = binding(component("account", TEXT), component("renamed_id", EXACT_NUMBER));
        var expected = key(ValidationKey.Part.text(" X "), ValidationKey.Part.number(new BigInteger("9007199254740993")));
        assertThat(bound(source, Map.of("tenant", " X ", "id", new BigInteger("9007199254740993")))).isEqualTo(expected);
        assertThat(bound(target, Map.of("account", " X ", "renamed_id", new BigDecimal("9007199254740993.000")))).isEqualTo(expected);
        assertThat(bound(target, Map.of("account", " X ", "renamed_id", new BigDecimal("9007199254740992")))).isNotEqualTo(expected);
        assertThat(bound(source, Map.of("tenant", "X", "id", new BigInteger("9007199254740993")))).isNotEqualTo(expected);
    }

    @Test void missingNullAndBadParentsAreLocatedWithoutInventingKeys() {
        var fields = new HashMap<String, Object>();
        fields.put("null", null);
        fields.put("parent", 7);
        var extractor = binding(component("missing", TEXT), component("null", EXACT_NUMBER),
                new Component(List.of("parent", "id"), EXACT_NUMBER, Set.of()));
        var result = (Invalid) extractor.read(row(fields));
        assertThat(result.problems()).containsExactly(new Problem(0, List.of("missing"), ProblemKind.MISSING_FIELD),
                new Problem(1, List.of("null"), ProblemKind.NULL_COMPONENT),
                new Problem(2, List.of("parent", "id"), ProblemKind.INVALID_PARENT));
    }

    @Test void literalDottedColumnsAreDifferentFromExplicitNestedPaths() {
        var extractor = binding(component("address.id", EXACT_NUMBER),
                new Component(List.of("address", "id"), EXACT_NUMBER, Set.of()));
        assertThat(bound(extractor, Map.of("address.id", 5, "address", Map.of("id", 9))))
                .isEqualTo(key(ValidationKey.Part.number(5), ValidationKey.Part.number(9)));
    }

    @Test void typedValuesNeedFixedLogicalAdmissionEvenForNullAndAfterEarlierMissing() {
        var accepted = binding(new Component(List.of("id"), EXACT_NUMBER, Set.of("number")));
        assertThat(bound(accepted, Map.of("id", new ValidationValues.Typed("number", "int64", "codec", 7L))))
                .isEqualTo(key(ValidationKey.Part.number(7)));
        assertThat(((Invalid) accepted.read(row(Map.of("id", new ValidationValues.Typed("number", null, "codec", null)))))
                .problems()).extracting(Problem::kind).containsExactly(ProblemKind.NULL_COMPONENT);
        for (Object value : new Object[] {7, null}) {
            var raw = Map.of("id", new ValidationValues.Typed("unregistered", "custom", "codec", value));
            assertThatThrownBy(() -> accepted.read(row(raw))).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
            assertThatThrownBy(() -> binding(component("missing", TEXT), component("id", EXACT_NUMBER)).read(row(raw)))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test void nestedTypedContainersNeedIndependentAdmissionAndPreserveInnerLogicalChecks() {
        var components = List.of(new Component(List.of("document", "id"), EXACT_NUMBER, Set.of("number")));
        var data = row(Map.of("document", new ValidationValues.Typed("json", "jsonb", "codec",
                Map.of("id", new ValidationValues.Typed("number", null, "codec", 11)))));
        assertThatThrownBy(() -> new ValidationKeyBinding("binding", components, Set.of(), LIMITS).read(data))
                .isInstanceOf(TapstateException.class);
        assertThat(((Bound) new ValidationKeyBinding("binding", components, Set.of("json"), LIMITS).read(data)).key())
                .isEqualTo(key(ValidationKey.Part.number(11)));
    }

    @Test void mismatchedRepresentationsAreDataProblemsWithoutParsingOrStringifying() {
        var extractor = binding(component("numeric", EXACT_NUMBER), component("text", TEXT), component("instant", INSTANT));
        var result = (Invalid) extractor.read(row(Map.of("numeric", "123", "text", 123, "instant", LocalDateTime.of(2026, 1, 2, 3, 4))));
        assertThat(result.problems()).extracting(Problem::kind).containsOnly(ProblemKind.TYPE_MISMATCH);
        assertThat(result.problems()).hasSize(3);
    }

    @Test void binarySubtypeAndTemporalCategoriesAreExact() {
        byte[] payload = {0, 1, (byte) 255};
        var date = LocalDate.of(2026, 10, 21);
        var time = LocalTime.of(12, 34, 56, 123456789);
        var local = date.atTime(time);
        var instant = Instant.parse("2026-10-21T12:34:56.123456789Z");
        var extractor = binding(component("bytes", BINARY), component("date", DATE), component("time", TIME),
                component("local", LOCAL_DATE_TIME), component("instant", INSTANT), component("flag", BOOLEAN));
        var actual = bound(extractor, Map.of("bytes", new ValidationValues.Binary(4, payload), "date", date,
                "time", time, "local", local, "instant", instant, "flag", true));
        payload[0] = 19;
        assertThat(actual).isEqualTo(key(ValidationKey.Part.binary(4, new byte[] {0, 1, (byte) 255}),
                ValidationKey.Part.date(date), ValidationKey.Part.time(time), ValidationKey.Part.localDateTime(local),
                ValidationKey.Part.instant(instant), ValidationKey.Part.bool(true)));
        assertThatThrownBy(() -> binding(component("id", BINARY)).read(row(Map.of("id", new ValidationValues.Binary(-1, new byte[] {1})))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
    }

    @Test void binaryFloatRequiresExplicitExactPolicyAndDoesNotCollapseDecimalNeighbors() {
        var defaultPolicy = binding(component("id", EXACT_NUMBER));
        assertThat(((Invalid) defaultPolicy.read(row(Map.of("id", 0.1d)))).problems())
                .extracting(Problem::kind).containsExactly(ProblemKind.TYPE_MISMATCH);
        var exactBinary = binding(new Component(List.of("id"), EXACT_NUMBER, Set.of(), NumericPolicy.EXACT_BINARY_FLOAT));
        assertThat(bound(exactBinary, Map.of("id", 0.1d))).isEqualTo(key(ValidationKey.Part.number(new BigDecimal(0.1d))))
                .isNotEqualTo(key(ValidationKey.Part.number(new BigDecimal("0.1"))));
        assertThat(bound(exactBinary, Map.of("id", 0.1f))).isEqualTo(key(ValidationKey.Part.number(new BigDecimal((double) 0.1f))))
                .isNotEqualTo(bound(exactBinary, Map.of("id", 0.1d)));
        assertThat(bound(exactBinary, Map.of("id", -0.0d))).isEqualTo(bound(exactBinary, Map.of("id", 0.0d)));
        for (double bad : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThat(((Invalid) exactBinary.read(row(Map.of("id", bad)))).problems())
                    .extracting(Problem::kind).containsExactly(ProblemKind.NON_FINITE);
        }
    }

    @Test void malformedTextIsADataViolationButKeyResourceLimitsStillFail() {
        var text = binding(component("id", TEXT));
        assertThat(((Invalid) text.read(row(Map.of("id", "\uD800")))).problems())
                .extracting(Problem::kind).containsExactly(ProblemKind.INVALID_TEXT);
        assertThat(bound(text, Map.of("id", "\uD800\uDC00"))).isEqualTo(key(ValidationKey.Part.text("\uD800\uDC00")));
        assertThatThrownBy(() -> text.read(row(Map.of("id", "x".repeat(9000)))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
    }

    @Test void totalKeyBudgetCannotHideBehindAnEarlierMissingComponent() {
        var components = new ArrayList<Component>();
        components.add(component("missing", TEXT));
        var data = new HashMap<String, Object>();
        for (int i = 0; i < 5; i++) {
            components.add(component("k" + i, TEXT));
            data.put("k" + i, "x".repeat(7000));
        }
        var binding = new ValidationKeyBinding("key", components, Set.of(), LIMITS);
        assertThatThrownBy(() -> binding.read(row(data))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
        var tiny = new ValidationKeyBinding("key", List.of(component("missing", TEXT)), Set.of(), new ValidationValues.Limits(1, 1, 1));
        assertThatThrownBy(() -> tiny.read(row(Map.of()))).isInstanceOf(TapstateException.class);
    }

    @Test void declarationsAreImmutableAndRejectAmbiguousDuplicatePaths() {
        var path = new ArrayList<>(List.of("id"));
        var component = new Component(path, TEXT, Set.of());
        path.clear();
        assertThat(component.path()).containsExactly("id");
        assertThatThrownBy(() -> binding(component, component)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Component(List.of("id"), TEXT, Set.of(), NumericPolicy.EXACT_BINARY_FLOAT))
                .isInstanceOf(TapstateException.class);
    }

    private static Component component(String field, ValidationKey.Kind kind) { return new Component(List.of(field), kind, Set.of()); }
    private static ValidationKeyBinding binding(Component... components) {
        return new ValidationKeyBinding("test-binding", List.of(components), Set.of(), LIMITS);
    }
    private static ValidationKey bound(ValidationKeyBinding binding, Map<String, ?> fields) { return ((Bound) binding.read(row(fields))).key(); }
    private static ValidationValues.Row row(Map<String, ?> data) { return ValidationValues.ownRow(data, LIMITS); }
    private static ValidationKey key(ValidationKey.Part... parts) { return new ValidationKey(List.of(parts)); }
}
