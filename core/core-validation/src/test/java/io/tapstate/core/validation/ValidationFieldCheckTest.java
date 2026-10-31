package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationFieldCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 150, 20);
    private static final ValidationValueComparison.TraversalLimits LIMITS = new ValidationValueComparison.TraversalLimits(ROW, 10000);

    @Test void onlyTheFixedGeneratedRootIdentifierIsExemptAndEveryBusinessFieldStillCompares() {
        var source = row(Map.of("id", 7, "name", "A", "body", Map.of("_id", "business-source")));
        var target = row(Map.of("_id", text("generated"), "id", 7, "name", "B", "body", Map.of("_id", "business-target"), "extra", 9));
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            var result = check(observation("run"), List.of(), source, target, policy(), LIMITS, root);
            assertThat(result.generatedIdValueExempt()).isTrue();
            assertThat(result.differences()).isEqualTo(3);
            var checked = finish(root);
            assertThat(checked.counts()).containsEntry(Kind.TARGET_VALUE, 2L).containsEntry(Kind.TARGET_EXTRA, 1L);
            assertThat(checked.findings().keySet()).anySatisfy(finding -> assertThat(finding.location().fieldPath()).containsExactly("body", "_id"));
            assertThat(checked.samples()).hasSize(1);
            assertThat(checked.samplesTruncated()).isTrue();
            assertThat(target.fields()).containsKey("_id");
        }
    }

    @Test void matchingRowsWithGeneratedIdCanAgreeButANameOrPrintedIdAloneNeverAuthorizesExemption() {
        var source = row(Map.of("id", 7));
        var target = row(Map.of("id", 7, "_id", "0123456789abcdef01234567"));
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            assertThat(check(observation("run"), List.of(), source, target, policy(), LIMITS, root).differences()).isZero();
            assertThat(finish(root).observedAgreement()).isTrue();
        }
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            var result = check(observation("run"), List.of(), source, target, Optional.empty(), LIMITS, root);
            assertThat(result.generatedIdValueExempt()).isFalse();
            assertThat(finish(root).counts()).containsOnlyKeys(Kind.TARGET_EXTRA);
        }
        assertThat(ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", false, false, false, ValidationFieldCheckTest::textId)).isEmpty();
        assertThat(ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", true, true, false, ValidationFieldCheckTest::textId)).isEmpty();
        assertThat(ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", true, false, true, ValidationFieldCheckTest::textId)).isEmpty();
    }

    @Test void sourceProvidedIdsIncludingNullStillUseOrdinaryValueComparison() {
        for (var source : List.of(row(Map.of("_id", "explicit", "id", 7)), row(nullableId()))) {
            var checkedType = new AtomicInteger();
            var policy = ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", true, false, false, value -> { checkedType.incrementAndGet(); return true; });
            try (var root = builder(ValidationMethod.FULL_COLUMN)) {
                var result = check(observation("run"), List.of(), source, row(Map.of("_id", "different", "id", 7)), policy, LIMITS, root);
                assertThat(result.generatedIdValueExempt()).isFalse();
                assertThat(result.differences()).isEqualTo(1);
                assertThat(finish(root).counts()).containsOnlyKeys(Kind.TARGET_VALUE);
            }
            assertThat(checkedType).hasValue(0);
        }
    }

    @Test void generatedIdStillHasRequiredPresenceAndAdmittedTypeObligations() {
        for (var target : List.of(row(Map.of("id", 7)), row(nullableId()), row(Map.of("id", 7, "_id", 99)))) {
            try (var root = builder(ValidationMethod.FULL_COLUMN)) {
                assertThat(check(observation("run"), List.of(), row(Map.of("id", 7)), target, policy(), LIMITS, root).differences()).isEqualTo(1);
                var checked = finish(root);
                assertThat(checked.counts()).containsOnlyKeys(target.fields().containsKey("_id") ? Kind.TARGET_TYPE : Kind.TARGET_MISSING);
                assertThat(checked.notApplicableScopes()).hasSize(2);
            }
        }
    }

    @Test void generatedIdPermissionCannotMoveToAnotherRunOrANestedBusinessDocument() {
        for (boolean stale : List.of(true, false)) {
            try (var root = builder(ValidationMethod.FULL_COLUMN)) {
                var policy = stale ? ValidationFieldCheck.generatedId(observation("old-run").run(), "mapping", "output", true, false, false, ValidationFieldCheckTest::textId) : policy();
                assertThatThrownBy(() -> check(observation("run"), stale ? List.of() : List.of("customer"), row(Map.of()), row(Map.of("_id", "value")), policy, LIMITS, root))
                        .isInstanceOf(TapstateException.class);
                assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
            }
        }
    }

    @Test void allOrdinaryDifferencesKeepTheirKindsAndOriginalDiagnosticRows() {
        var source = row(Map.of("missing", 7, "type", "text", "value", 1, "", "empty-name"));
        var target = row(Map.of("extra", 8, "type", 9, "value", 2, "", "changed"));
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            var result = check(observation("run"), List.of("payload"), source, target, Optional.empty(), LIMITS, root);
            assertThat(result.differences()).isEqualTo(5);
            var checked = finish(root);
            assertThat(checked.counts()).containsEntry(Kind.TARGET_MISSING, 1L).containsEntry(Kind.TARGET_EXTRA, 1L)
                    .containsEntry(Kind.TARGET_TYPE, 1L).containsEntry(Kind.TARGET_VALUE, 2L);
            assertThat(checked.samples().getFirst().source().orElseThrow().row()).isSameAs(source);
            assertThat(checked.samples().getFirst().target().orElseThrow().row()).isSameAs(target);
            assertThat(checked.findings().keySet()).allSatisfy(finding -> assertThat(finding.location().fieldPath().getFirst()).isEqualTo("payload"));
        }
    }

    @Test void dataEqualityDoesNotBypassReadProvenanceOrTheFullColumnMethod() {
        var equal = row(Map.of("id", 7));
        try (var root = ValidationRootCheck.builder(observation("run"), ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024 * 1024, 100, 100, 20, 1))) {
            assertThatThrownBy(() -> check(observation("run"), List.of(), equal, equal, Optional.empty(), LIMITS, root)).isInstanceOf(TapstateException.class);
        }
        try (var root = builder(ValidationMethod.UNIQUE_KEY)) {
            assertThatThrownBy(() -> check(observation("run"), List.of(), equal, equal, Optional.empty(), LIMITS, root)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void typeRuleAndTraversalFailuresDiscardPreviouslyStagedFacts() {
        var source = row(Map.of("id", 7));
        var target = row(Map.of("id", 8, "_id", "generated"));
        var fatal = new AssertionError("type rule failed");
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            var policy = ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", true, false, false, value -> { throw fatal; });
            assertThat(catchThrowable(() -> check(observation("run"), List.of(), source, target, policy, LIMITS, root))).isSameAs(fatal);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
        try (var root = builder(ValidationMethod.FULL_COLUMN)) {
            assertThatThrownBy(() -> check(observation("run"), List.of(), source, row(Map.of("id", 8)), policy(),
                    new ValidationValueComparison.TraversalLimits(ROW, 1), root)).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void generatedIdRuleCannotBeBorrowedByAnotherMappingInTheSameRun() {
        var original = observation("run");
        var other = new ValidationObservation(original.run(), new ValidationRoot("other-mapping", "output", key(), List.of()), 1);
        try (var root = ValidationRootCheck.builder(other, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new Limits(1024 * 1024, 100, 100, 20, 1))) {
            for (String side : List.of("source", "target")) root.read(side.equals("source") ? Side.SOURCE : Side.TARGET,
                    new ValidationCheckEvent.ReadBoundary(side, "complete", Instant.EPOCH, Instant.EPOCH));
            assertThatThrownBy(() -> check(other, List.of(), row(Map.of("id", 7)), row(Map.of("id", 7, "_id", "unrelated")), policy(), LIMITS, root))
                    .isInstanceOf(TapstateException.class);
        }
    }

    private static ValidationFieldCheck.Result check(ValidationObservation observation, List<Object> path, ValidationValues.Row source,
            ValidationValues.Row target, Optional<ValidationFieldCheck.GeneratedId> policy, ValidationValueComparison.TraversalLimits limits, Builder root) {
        return ValidationFieldCheck.check(observation, path, source, target, Optional.of(sample("source", source)), Optional.of(sample("target", target)),
                policy, String::equals, limits, root);
    }
    private static Optional<ValidationFieldCheck.GeneratedId> policy() {
        return ValidationFieldCheck.generatedId(observation("run").run(), "mapping", "output", true, false, false, ValidationFieldCheckTest::textId);
    }
    private static boolean textId(Object value) {
        while (value instanceof ValidationValues.Typed typed) {
            if (!typed.logicalType().equals("text")) return false;
            value = typed.value();
        }
        return value instanceof String;
    }
    private static ValidationValues.Typed text(String value) { return new ValidationValues.Typed("text", null, "codec", value); }
    private static ValidationValues.Row row(Map<String, ?> values) { return ValidationValues.ownRow(values, ROW); }
    private static Map<String, Object> nullableId() { var fields = new HashMap<String, Object>(); fields.put("_id", null); fields.put("id", 7); return fields; }
    private static SampledSide sample(String side, ValidationValues.Row row) {
        return new SampledSide(new ValidationPhysicalIdentity(side, "db", null, "table", List.of(List.of("id")), key()), row, side);
    }
    private static Builder builder(ValidationMethod method) {
        var root = ValidationRootCheck.builder(observation("run"), method, ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT,
                new Limits(1024 * 1024, 100, 100, 20, 1));
        for (String side : List.of("source", "target")) root.read(side.equals("source") ? Side.SOURCE : Side.TARGET,
                new ValidationCheckEvent.ReadBoundary(side, "complete", Instant.EPOCH, Instant.EPOCH));
        return root;
    }
    private static ValidationRootCheck finish(Builder root) { for (Check check : Check.values()) root.checked(check); return root.finish(); }
    private static ValidationObservation observation(String run) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", run, ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(), List.of()), 1);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
}
