package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationNestFieldCheck.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestFieldCheckTest {
    private static final ValidationNestFieldCheck.Limits LIMITS = new ValidationNestFieldCheck.Limits(1024 * 1024, 1024 * 1024, 100, 100);

    @Test void disjointFlatFieldsAndEmptyFlatContributionsLeaveOtherFieldsComparable() {
        var fields = List.of(field("parent", FieldKind.PARENT, "id"), field("invoice", FieldKind.FLAT, "invoice_id"),
                field("profile", FieldKind.FLAT, "name"), field("address", FieldKind.EMBED_PATH, "address", "city"));
        var builder = builder(ValidationMethod.FULL_COLUMN);
        var result = check(observation(1), fields, LIMITS, builder);
        assertThat(result.conflicts()).isEmpty();
        assertThat(result.uncontested()).containsExactlyElementsOf(fields);
        assertThat(finish(builder, ValidationMethod.FULL_COLUMN).observedAgreement()).isTrue();
        var empty = builder(ValidationMethod.FULL_COLUMN);
        assertThat(check(observation(1), List.of(fields.getFirst()), LIMITS, empty).conflicts()).isEmpty();
        assertThat(finish(empty, ValidationMethod.FULL_COLUMN).observedAgreement()).isTrue();
    }

    @Test void equalFieldsConflictRegardlessOfValuesOrDeclarationOrderAndUnrelatedFieldsSurvive() {
        var parent = field("parent", FieldKind.PARENT, "contact");
        var flat = field("profile", FieldKind.FLAT, "contact");
        var safe = field("profile", FieldKind.FLAT, "age");
        for (var fields : List.of(List.of(parent, flat, safe), List.of(safe, flat, parent))) {
            var builder = builder(ValidationMethod.FULL_COLUMN);
            var result = check(observation(1), fields, LIMITS, builder);
            assertThat(result.conflicts()).containsExactly(new Conflict(flat, parent));
            assertThat(result.uncontested()).containsExactly(safe);
            var root = finish(builder, ValidationMethod.FULL_COLUMN);
            assertThat(root.counts()).containsOnlyKeys(Kind.SOURCE_FIELD_CONFLICT);
            assertThat(root.notCompared()).hasSize(2).allSatisfy(blocked ->
                    assertThat(blocked.location().fieldPath()).containsExactly("contact"));
            assertThat(root.observedAgreement()).isFalse();
        }
    }

    @Test void declaredObjectOrArrayPathsStayReservedEvenWhenTheirCurrentContentsAreEmpty() {
        var flat = field("profile", FieldKind.FLAT, "address");
        var reserved = field("empty-sibling", FieldKind.EMBED_PATH, "address", "city");
        var builder = builder(ValidationMethod.FULL_COLUMN);
        var result = check(observation(1), List.of(flat, reserved), LIMITS, builder);
        assertThat(result.conflicts()).containsExactly(new Conflict(flat, reserved));
        var root = finish(builder, ValidationMethod.FULL_COLUMN);
        assertThat(root.notCompared()).hasSize(4);
        assertThat(root.notCompared()).anySatisfy(blocked -> assertThat(blocked.location().fieldPath()).containsExactly("address", "city"));
    }

    @Test void dottedOverlapPreservesActualLiteralDiagnosticPathsAndSegmentBoundaries() {
        var flat = field("profile", FieldKind.FLAT, "a.b");
        for (Field other : List.of(field("parent", FieldKind.PARENT, "a"), field("other", FieldKind.FLAT, "a.b.c"),
                field("nested", FieldKind.EMBED_PATH, "a", "b"))) {
            var builder = builder(ValidationMethod.FULL_COLUMN);
            assertThat(check(observation(1), List.of(flat, other), LIMITS, builder).conflicts()).hasSize(1);
            assertThat(finish(builder, ValidationMethod.FULL_COLUMN).findings().keySet()).singleElement().satisfies(finding ->
                    assertThat(finding.location().fieldPath()).containsExactly("a.b"));
        }
        var builder = builder(ValidationMethod.FULL_COLUMN);
        assertThat(check(observation(1), List.of(flat, field("parent", FieldKind.PARENT, "a.bc")), LIMITS, builder).conflicts()).isEmpty();
        assertThat(finish(builder, ValidationMethod.FULL_COLUMN).observedAgreement()).isTrue();
    }

    @Test void nestedPathsConflictWithTheirOwnFlatRowButLiteralFieldsWithinOneRowAreNotSeparateWriters() {
        var builder = builder(ValidationMethod.FULL_COLUMN);
        var fields = List.of(field("profile", FieldKind.FLAT, "contact"), field("profile", FieldKind.FLAT, "contact.phone"),
                field("profile", FieldKind.EMBED_PATH, "contact", "address"));
        var result = check(observation(1), fields, LIMITS, builder);
        assertThat(result.conflicts()).containsExactly(new Conflict(fields.get(0), fields.get(2)));
        assertThat(result.uncontested()).containsExactly(fields.get(1));
        finish(builder, ValidationMethod.FULL_COLUMN);
    }

    @Test void allCompetingOwnersAreRetainedWithoutDuplicateFindingCountsOrAWinner() {
        var fields = List.of(field("parent", FieldKind.PARENT, "a"), field("flat1", FieldKind.FLAT, "a.b"), field("flat2", FieldKind.FLAT, "a.b"));
        for (boolean reverse : List.of(false, true)) {
            var ordered = new ArrayList<>(fields);
            if (reverse) Collections.reverse(ordered);
            var builder = builder(ValidationMethod.FULL_COLUMN);
            var result = check(observation(1), ordered, LIMITS, builder);
            assertThat(result.conflicts()).hasSize(3);
            assertThat(result.uncontested()).isEmpty();
            assertThat(finish(builder, ValidationMethod.FULL_COLUMN).counts()).containsEntry(Kind.SOURCE_FIELD_CONFLICT, 1L);
        }
    }

    @Test void byteWorkAndConflictBudgetsAbortStagingInsteadOfReturningPartialResults() {
        var fields = List.of(field("parent", FieldKind.PARENT, "a"), field("flat1", FieldKind.FLAT, "a"), field("flat2", FieldKind.FLAT, "a"));
        var limits = List.of(new ValidationNestFieldCheck.Limits(1, 10000, 10, 10),
                new ValidationNestFieldCheck.Limits(10000, 1, 10, 10), new ValidationNestFieldCheck.Limits(10000, 10000, 2, 10),
                new ValidationNestFieldCheck.Limits(10000, 10000, 10, 1));
        for (var bounded : limits) {
            var builder = builder(ValidationMethod.FULL_COLUMN);
            assertThatThrownBy(() -> check(observation(1), fields, bounded, builder)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void oldObservationsAndDuplicateInputClaimsCannotCompleteARoot() {
        var builder = builder(ValidationMethod.FULL_COLUMN);
        assertThatThrownBy(() -> check(observation(2), List.of(), LIMITS, builder)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
        var duplicate = builder(ValidationMethod.FULL_COLUMN);
        var field = field("profile", FieldKind.FLAT, "name");
        assertThatThrownBy(() -> check(observation(1), List.of(field, field), LIMITS, duplicate)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(duplicate::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void nestedAncestorScopeAndUniqueKeyModeStayExplicit() {
        var original = observation(1);
        var parent = new ValidationObservation(original.run(), new ValidationRoot("mapping", "output", original.root().rootKey(),
                List.of(new ValidationRoot.Ancestor(List.of("items"), key(11)))), 1);
        var builder = builder(ValidationMethod.UNIQUE_KEY);
        check(parent, List.of(field("parent", FieldKind.PARENT, "id"), field("flat", FieldKind.FLAT, "id")), LIMITS, builder);
        var root = finish(builder, ValidationMethod.UNIQUE_KEY);
        assertThat(root.checks()).containsEntry(Check.VALUES, Status.NOT_APPLICABLE);
        assertThat(root.notCompared()).singleElement().satisfies(blocked -> assertThat(blocked.location().scope()).isEqualTo(parent.root()));
    }

    @Test void emptyJsonMemberNamesAreLiteralFieldsRatherThanTheWholeDocument() {
        var builder = builder(ValidationMethod.FULL_COLUMN);
        check(observation(1), List.of(field("parent", FieldKind.PARENT, ""), field("flat", FieldKind.FLAT, ""),
                field("safe", FieldKind.FLAT, "name")), LIMITS, builder);
        var root = finish(builder, ValidationMethod.FULL_COLUMN);
        assertThat(root.findings().keySet()).singleElement().satisfies(finding -> {
            assertThat(finding.location().fieldPath()).containsExactly("");
            assertThat(finding.location().wholeRoot()).isFalse();
        });
    }

    private static Field field(String owner, FieldKind kind, String... path) { return new Field(owner, kind, List.of(path)); }
    private static Builder builder(ValidationMethod method) {
        return ValidationRootCheck.builder(observation(1), method, ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT,
                new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 4))
                .read(Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "complete", Instant.EPOCH, Instant.EPOCH))
                .read(Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "complete", Instant.EPOCH, Instant.EPOCH));
    }
    private static ValidationRootCheck finish(Builder builder, ValidationMethod method) {
        builder.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE);
        if (method == ValidationMethod.FULL_COLUMN) builder.checked(Check.VALUES);
        return builder.finish();
    }
    private static ValidationObservation observation(long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), List.of()), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
