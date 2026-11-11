package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestArrayFieldCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(65536, 1000, 20);
    private static final ValidationNestTargetArray.Limits ARRAY = new ValidationNestTargetArray.Limits(ROW, 100, 10000);
    private static final ValidationKeyBinding KEY = new ValidationKeyBinding("id", List.of(
            new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), ROW);
    private static final ValidationObservation OBSERVATION = observation(List.of(), 1);

    @Test void reorderedNestArraysPassWhileOrdinaryArraysAndIndependentValuesStillCompare() {
        var left = row(Map.of("label", "known", "ordinary", List.of(1, 2), "items", List.of(item(11, "A"), item(12, "B"))));
        for (boolean wrong : List.of(false, true)) {
            var right = side(Map.of("label", wrong ? "wrong" : "known", "ordinary", wrong ? List.of(2, 1) : List.of(1, 2),
                    "items", List.of(item(12, "B"), item(11, "A"))));
            try (var root = builder()) {
                var checked = array(OBSERVATION, List.of("items"), left, right, right.row(),
                        List.of(item(11, "A"), item(12, "B")), root, group -> fields(group, root));
                assertThat(compare(OBSERVATION, left, right.row(), List.of(), Set.of(), List.of(checked), root).differences())
                        .isEqualTo(wrong ? 3 : 0);
                var result = finish(root);
                assertThat(result.observedAgreement()).isEqualTo(!wrong);
                assertThat(result.notCompared()).isEmpty();
                if (wrong) assertThat(result.findings().keySet()).extracting(finding -> finding.location().fieldPath())
                        .containsExactlyInAnyOrder(List.of("label"), List.of("ordinary", 0), List.of("ordinary", 1));
            }
        }
    }

    @Test void malformedContainersAndUnkeyedMembersStayDiagnosedWithoutFabricatedMissingMembers() {
        var left = row(Map.of("label", "expected"));
        var variants = List.of(Map.<String, Object>of(), nullable("items"), Map.<String, Object>of("items", "bad"),
                Map.<String, Object>of("items", List.of(Map.of("value", "unkeyed"))));
        for (var variant : variants) {
            var values = new HashMap<>(variant); values.put("label", "wrong"); var right = side(values);
            try (var root = builder()) {
                var checked = array(OBSERVATION, List.of("items"), left, right, right.row(),
                        List.of(item(11, "A"), item(11, "B")), root, group -> fail("No unique member is available"));
                compare(OBSERVATION, left, right.row(), List.of(), Set.of(), List.of(checked), root);
                var result = finish(root);
                Kind target = variant.get("items") instanceof List<?> ? Kind.TARGET_KEY_INVALID : Kind.TARGET_STRUCTURE;
                assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(target, 1L, Kind.SOURCE_DUPLICATE, 1L, Kind.TARGET_VALUE, 1L));
                assertThat(result.notCompared()).hasSize(2).allSatisfy(blocked ->
                        assertThat(blocked.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11))));
            }
        }
    }

    @Test void membershipAndMemberValueFindingsAreNotLostWhenTheParentSubtreeIsDelegated() {
        var left = row(Map.of("label", "same"));
        var right = side(Map.of("label", "same", "items", List.of(item(12, "B"), item(12, "B"), item(13, "extra"), item(14, "wrong"))));
        try (var root = builder()) {
            var checked = array(OBSERVATION, List.of("items"), left, right, right.row(),
                    List.of(item(11, "A"), item(12, "B"), item(14, "expected")), root, group -> fields(group, root));
            compare(OBSERVATION, left, right.row(), List.of(), Set.of(), List.of(checked), root);
            assertThat(finish(root).counts()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    Kind.TARGET_MISSING, 1L, Kind.TARGET_EXTRA, 1L, Kind.TARGET_DUPLICATE, 1L, Kind.TARGET_VALUE, 1L));
        }
    }

    @Test void multipleLevelsAndSiblingsKeepTheirWholeAncestorIdentity() {
        var first = Map.<String, Object>of("id", 11, "allocations", List.of(item(501, "A"), item(502, "B")));
        var second = Map.<String, Object>of("id", 12, "allocations", List.of(item(501, "other-parent")));
        var left = row(Map.of("items", List.of(first, second), "shipping", List.of(item(11, "separate-path"))));
        for (boolean wrong : List.of(false, true)) {
            var right = side(Map.of("items", List.of(second,
                    Map.of("id", 11, "allocations", List.of(item(502, "B"), item(501, wrong ? "wrong" : "A")))),
                    "shipping", List.of(item(11, "separate-path"))));
            try (var root = builder()) {
                var checked = array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(first, second), root, group -> {
                    var source = group.sourceSamples().getFirst(); var target = group.targetSamples().getFirst();
                    var child = new ValidationObservation(OBSERVATION.run(), group.scope(), OBSERVATION.revision());
                    @SuppressWarnings("unchecked") var allocations = (List<Map<String, Object>>) source.values().fields().get("allocations");
                    var allocationCheck = array(child, List.of("allocations"), source.values(), right, target.values(), allocations,
                            root, allocation -> fields(allocation, root));
                    compare(child, source.values(), target.values(), List.of(), Set.of(), List.of(allocationCheck), root);
                });
                var shipping = array(OBSERVATION, List.of("shipping"), left, right, right.row(),
                        List.of(item(11, "separate-path")), root, group -> fields(group, root));
                compare(OBSERVATION, left, right.row(), List.of(), Set.of(), List.of(checked, shipping), root);
                var result = finish(root);
                assertThat(result.observedAgreement()).isEqualTo(!wrong);
                if (wrong) assertThat(result.findings().keySet()).singleElement().satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo(Kind.TARGET_VALUE);
                    assertThat(finding.location().scope().ancestors()).containsExactly(
                            new ValidationRoot.Ancestor(List.of("items"), key(11)), new ValidationRoot.Ancestor(List.of("allocations"), key(501)));
                    assertThat(finding.location().fieldPath()).containsExactly("value");
                });
            }
        }
    }

    @Test void evidenceCannotBeReusedForAnotherBuilderObservationOrWorkingRow() {
        var left = row(Map.of()); var right = side(Map.of("items", List.of()));
        for (int scenario = 0; scenario < 6; scenario++) {
            try (var root = builder(); var other = builder()) {
                var checked = array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(), root, group -> fail("Empty array"));
                var parent = scenario == 0 ? observation(List.of(), 2) : scenario == 1
                        ? observation(List.of(new ValidationRoot.Ancestor(List.of("parent"), key(9))), 1) : OBSERVATION;
                var source = scenario == 2 ? row(left.fields()) : left;
                var target = scenario == 3 ? row(right.row().fields()) : right.row();
                var used = scenario == 4 ? other : root;
                var checks = scenario == 5 ? List.of(checked, checked) : List.of(checked);
                assertThatThrownBy(() -> compare(parent, source, target, List.of(), Set.of(), checks, used))
                        .isInstanceOf(TapstateException.class);
                assertThatThrownBy(used::finish).isInstanceOfAny(IllegalStateException.class, TapstateException.class);
            }
        }
    }

    @Test void sourceUnknownPathsRemainSeparateAndCannotOverlapADelegatedArray() {
        var left = row(Map.of("known", 1)); var right = side(Map.of("known", 2, "items", List.of(), "unknown", 8));
        for (var unknown : List.<List<Object>>of(List.of("unknown"), List.of("items"), List.of("items", 0), List.of())) {
            try (var root = builder()) {
                var checked = array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(), root, group -> fail("Empty array"));
                block(root, unknown);
                if (unknown.equals(List.of("unknown"))) {
                    compare(OBSERVATION, left, right.row(), List.of(unknown), Set.of(), List.of(checked), root);
                    assertThat(finish(root).counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_INVALID, 1L, Kind.TARGET_VALUE, 1L));
                } else assertThatThrownBy(() -> compare(OBSERVATION, left, right.row(), List.of(unknown), Set.of(), List.of(checked), root))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("field", "fieldCheck.arrayOverlap"));
            }
        }
        try (var root = builder()) {
            var checked = array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(), root, group -> fail("Empty array"));
            block(root, List.of());
            compare(OBSERVATION, left, right.row(), List.of(List.of()), Set.of("items", "known"), List.of(checked), root);
            assertThat(finish(root).counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_INVALID, 1L, Kind.TARGET_VALUE, 1L));
        }
    }

    @Test void callbackAndBudgetFailuresCannotReturnEvidenceOrCompleteTheRoot() {
        var left = row(Map.of()); var right = side(Map.of("items", List.of(item(11, "A"))));
        var failure = new IllegalStateException("member checker failed");
        try (var root = builder()) {
            assertThatThrownBy(() -> array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(item(11, "A")),
                    root, group -> { throw failure; })).isSameAs(failure);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
        try (var root = builder(); var members = members(OBSERVATION, List.of("items"), List.of())) {
            assertThatThrownBy(() -> ValidationNestArrayFieldCheck.check(OBSERVATION, List.of("items"), left, right, right.row(), KEY,
                    Optional.of(KEY), Set.of(), new ValidationNestTargetArray.Limits(ROW, 100, 1), members, root, group -> {}))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThatThrownBy(root::finish).isInstanceOfAny(IllegalStateException.class, TapstateException.class);
        }
    }

    @Test void evidenceMetadataSharesTheBoundedFieldMaskBudget() {
        var left = row(Map.of()); var right = side(Map.of()); var path = List.of("x".repeat(200));
        try (var root = builder()) {
            var checked = array(OBSERVATION, path, left, right, right.row(), List.of(), root, group -> fail("Empty array"));
            assertThatThrownBy(() -> ValidationFieldCheck.checkComposed(OBSERVATION, left, right.row(), Optional.empty(), Optional.empty(),
                    Optional.empty(), String::equals, new ValidationValueComparison.TraversalLimits(new ValidationValues.Limits(256, 100, 20), 10000),
                    List.of(), Set.of(), List.of(checked), root)).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.args()).containsEntry("limit", "fieldCheck.arrays"));
        }
    }

    @Test void evidenceValidationUsesTheSameOperationBudgetAsOrdinaryFields() {
        var left = row(Map.of()); var right = side(Map.of("items", List.of()));
        try (var root = builder()) {
            var checked = array(OBSERVATION, List.of("items"), left, right, right.row(), List.of(), root, group -> fail("Empty array"));
            assertThatThrownBy(() -> ValidationFieldCheck.checkComposed(OBSERVATION, left, right.row(), Optional.empty(), Optional.empty(),
                    Optional.empty(), String::equals, new ValidationValueComparison.TraversalLimits(ROW, 1), List.of(), Set.of(), List.of(checked), root))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "fieldCheck.arrayOperations"));
            assertThatThrownBy(root::finish).isInstanceOfAny(IllegalStateException.class, TapstateException.class);
        }
    }

    @Test void absentOrNonObjectSourcePrefixesCannotProduceCheckedArrayEvidence() {
        var right = side(Map.of("profile", Map.of("items", List.of())));
        for (var fields : List.of(Map.<String, Object>of(), nullable("profile"), Map.<String, Object>of("profile", "wrong"))) {
            try (var root = builder()) {
                assertThatThrownBy(() -> array(OBSERVATION, List.of("profile", "items"), row(fields), right, right.row(), List.of(), root,
                        group -> fail("Empty array"))).isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.args()).containsEntry("field", "fieldCheck.arraySourcePrefix"));
                assertThatThrownBy(root::finish).isInstanceOfAny(IllegalStateException.class, TapstateException.class);
            }
        }
    }

    @Test void knownSourcePrefixesPreserveIndependentSiblingChecksAndTheOptionalArrayLeaf() {
        var left = row(Map.of("profile", Map.of("label", "expected")));
        var right = side(Map.of("profile", Map.of("label", "wrong", "items", List.of(item(11, "A")))));
        try (var root = builder()) {
            var checked = array(OBSERVATION, List.of("profile", "items"), left, right, right.row(), List.of(item(11, "A")), root,
                    group -> fields(group, root));
            compare(OBSERVATION, left, right.row(), List.of(), Set.of(), List.of(checked), root);
            var result = finish(root);
            assertThat(result.counts()).containsExactly(entry(Kind.TARGET_VALUE, 1L));
            assertThat(result.findings().keySet()).singleElement().satisfies(finding ->
                    assertThat(finding.location().fieldPath()).containsExactly("profile", "label"));
        }
    }

    private static ValidationNestArrayFieldCheck.Checked array(ValidationObservation parent, List<String> path, ValidationValues.Row source,
            ValidationCheckEvent.Side physical, ValidationValues.Row target, List<Map<String, Object>> expected, Builder root,
            Consumer<ValidationNestArrayMembers.Group> pair) {
        try (var members = members(parent, path, expected)) {
            return ValidationNestArrayFieldCheck.check(parent, path, source, physical, target, KEY, Optional.of(KEY), Set.of(), ARRAY, members, root, pair);
        }
    }
    private static ValidationNestArrayMembers members(ValidationObservation parent, List<String> path, List<Map<String, Object>> expected) {
        var members = new ValidationNestArrayMembers(parent, path, new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW));
        for (var value : expected) {
            var id = ((ValidationKeyBinding.Bound) KEY.read(row(value))).key();
            members.add(ValidationNestArrayMembers.Side.SOURCE, parent, new ValidationNestArrayMembers.Member(id, id,
                    new ValidationPhysicalIdentity("source", "db", null, "children", List.of(List.of("id")), id), row(value)));
        }
        members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("children"));
        return members;
    }
    private static void fields(ValidationNestArrayMembers.Group group, Builder root) {
        var source = group.sourceSamples().getFirst(); var target = group.targetSamples().getFirst();
        ValidationFieldCheck.check(new ValidationObservation(OBSERVATION.run(), group.scope(), OBSERVATION.revision()), List.of(),
                source.values(), target.values(), Optional.of(new SampledSide(source.physical(), source.original(), "children")),
                Optional.of(new SampledSide(target.physical(), target.original(), "target")), Optional.empty(), String::equals,
                new ValidationValueComparison.TraversalLimits(ROW, 10000), root);
    }
    private static ValidationFieldCheck.Result compare(ValidationObservation parent, ValidationValues.Row source, ValidationValues.Row target,
            List<List<Object>> unknown, Set<String> defined, List<ValidationNestArrayFieldCheck.Checked> arrays, Builder root) {
        return ValidationFieldCheck.checkComposed(parent, source, target, Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                new ValidationValueComparison.TraversalLimits(ROW, 10000), unknown, defined, arrays, root);
    }
    private static void block(Builder root, List<Object> path) {
        var location = new Location(OBSERVATION.root(), path);
        var finding = new Finding(Kind.SOURCE_FIELD_INVALID, location);
        root.finding(finding).notCompared(Check.STRUCTURE, location, finding).notCompared(Check.VALUES, location, finding);
    }
    private static Builder builder() {
        return ValidationRootCheck.builder(OBSERVATION, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new Limits(4 * 1024 * 1024, 100, 100, 30, 10));
    }
    private static ValidationRootCheck finish(Builder root) {
        return root.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE).checked(Check.VALUES).finish();
    }
    private static ValidationCheckEvent.Side side(Map<String, ?> fields) {
        return new ValidationCheckEvent.Side(new ValidationPhysicalIdentity("target", "db", null, "roots", List.of(List.of("id")), key(102)),
                row(fields), 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
    }
    private static Map<String, Object> item(long id, String value) { return Map.of("id", id, "value", value); }
    private static Map<String, Object> nullable(String field) { var fields = new HashMap<String, Object>(); fields.put(field, null); return fields; }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH); }
    private static ValidationObservation observation(List<ValidationRoot.Ancestor> ancestors, long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), ancestors), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
