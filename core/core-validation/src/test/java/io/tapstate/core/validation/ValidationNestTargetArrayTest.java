package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestTargetArrayTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 200, 20);
    private static final ValidationNestTargetArray.Limits LIMITS = new ValidationNestTargetArray.Limits(ROW, 100, 2000);
    private static final ValidationKeyBinding KEY = new ValidationKeyBinding("id", List.of(
            new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of("json", "map"), ROW);

    @Test void actualTargetArrayOrderDoesNotChangeMembershipAndPhysicalDocumentIdentityRemainsRaw() {
        var parent = observation(List.of(), 1);
        var document = side(Map.of("id", 102, "items", List.of(Map.of("id", 12, "value", "B"), Map.of("id", 11, "value", "A"))));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            source(members, parent, 11, "A"); source(members, parent, 12, "B");
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThat(read(parent, List.of("items"), document, document.row(), Optional.of(KEY), LIMITS, members, root))
                    .isEqualTo(ValidationNestTargetArray.State.COMPLETE);
            var result = members.finish();
            ValidationNestArrayCheck.check(result, root, group -> {
                var target = group.targetSamples().getFirst();
                assertThat(target.original()).isSameAs(document.row());
                assertThat(target.physical()).isEqualTo(document.identity());
                assertThat(target.physical().actualKey()).isEqualTo(key(102));
                assertThat(ValidationValueComparison.compare(group.sourceSamples().getFirst().values(), target.values())).isEmpty();
            });
            assertThat(finish(root).observedAgreement()).isTrue();
        }
    }

    @Test void emptyArrayCompletesButMissingNullScalarOrContradictoryWrapperIsAStructureDifference() {
        var parent = observation(List.of(), 1);
        for (Object value : List.of(List.of(), new ValidationValues.Typed("json", null, "codec", List.of()))) {
            var doc = side(Map.of("items", value));
            try (var members = members(parent, List.of("items")); var root = builder()) {
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
                assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                        .isEqualTo(ValidationNestTargetArray.State.COMPLETE);
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
                assertThat(members.finish().groups()).isEmpty();
            }
        }
        for (var fields : List.of(Map.<String, Object>of(), nullable("items"), Map.<String, Object>of("items", "wrong"),
                Map.<String, Object>of("items", new ValidationValues.Typed("text", null, "codec", List.of())))) {
            var doc = side(fields);
            try (var members = members(parent, List.of("items")); var root = builder()) {
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
                assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                        .isEqualTo(ValidationNestTargetArray.State.INVALID);
                assertThatThrownBy(members::finish).isInstanceOf(IllegalStateException.class);
                root.read(Side.SOURCE, boundary("source"));
                assertThat(finish(root).counts()).containsOnlyKeys(Kind.TARGET_STRUCTURE);
            }
        }
    }

    @Test void unkeyedElementsDiscardMembershipInsteadOfCausingFalseMissingOrExtraFindings() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of(Map.of("id", 11), Map.of("value", "missing-key"), nullable("id"), Map.of("id", 12))));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            source(members, parent, 11, "A"); source(members, parent, 12, "B");
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                    .isEqualTo(ValidationNestTargetArray.State.INVALID);
            assertThatThrownBy(members::finish).isInstanceOf(IllegalStateException.class);
            root.read(Side.SOURCE, boundary("source"));
            var result = finish(root);
            assertThat(result.counts()).containsOnlyKeys(Kind.TARGET_KEY_INVALID).containsEntry(Kind.TARGET_KEY_INVALID, 2L);
            assertThat(result.findings().keySet()).anySatisfy(finding -> assertThat(finding.location().fieldPath()).containsExactly("items", 1, "id"));
            assertThat(result.notApplicableScopes()).allSatisfy(skipped -> assertThat(skipped.location().fieldPath()).containsExactly("items"));
            assertThat(result.samples()).allSatisfy(sample -> assertThat(sample.target().orElseThrow().row()).isSameAs(doc.row()));
        }
    }

    @Test void malformedElementsAndHiddenDeclaredRowKeysRemainDistinct() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of(Map.of("id", 11), "not-an-object")));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.empty(), LIMITS, members, root))
                    .isEqualTo(ValidationNestTargetArray.State.INVALID);
            root.read(Side.SOURCE, boundary("source"));
            assertThat(finish(root).findings().keySet()).singleElement().satisfies(finding ->
                    assertThat(finding.location().fieldPath()).containsExactly("items", 1));
        }
        var hidden = side(Map.of("items", List.of(Map.of("id", 11))));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            source(members, parent, 11, "A");
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            read(parent, List.of("items"), hidden, hidden.row(), Optional.empty(), LIMITS, members, root);
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThat(members.finish().groups().getFirst().rowIdentityComparison()).isEqualTo(ValidationNestArrayMembers.RowIdentityComparison.NOT_OBSERVABLE);
        }
    }

    @Test void nestedWorkingParentKeepsAllAncestorKeysAndTheActualRootRead() {
        var parent = observation(List.of(new ValidationRoot.Ancestor(List.of("items"), key(11))), 1);
        var child = Map.<String, Object>of("id", 11, "allocations", List.of(Map.of("id", 501)));
        var doc = side(Map.of("id", 102, "items", List.of(child)));
        try (var members = members(parent, List.of("allocations")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            read(parent, List.of("allocations"), doc, ValidationValues.ownRow(child, ROW), Optional.of(KEY), LIMITS, members, root);
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            var group = members.finish().groups().getFirst();
            assertThat(group.scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)),
                    new ValidationRoot.Ancestor(List.of("allocations"), key(501)));
            assertThat(group.targetSamples().getFirst().original()).isSameAs(doc.row());
        }
    }

    @Test void staleOrDifferentFramePathAndResourceFailuresDiscardAllStaging() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of(Map.of("id", 11), Map.of("id", 12))));
        for (int scenario = 0; scenario < 4; scenario++) {
            var actual = scenario == 0 ? observation(List.of(), 2) : parent;
            var limits = scenario == 2 ? new ValidationNestTargetArray.Limits(ROW, 1, 2000)
                    : scenario == 3 ? new ValidationNestTargetArray.Limits(ROW, 100, 1) : LIMITS;
            try (var members = members(parent, scenario == 1 ? List.of("other") : List.of("items")); var root = builder()) {
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
                assertThatThrownBy(() -> read(actual, List.of("items"), doc, doc.row(), Optional.of(KEY), limits, members, root))
                        .isInstanceOf(TapstateException.class);
                assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(members::finish).isInstanceOfAny(IllegalStateException.class, TapstateException.class);
            }
        }
    }

    @Test void evenEmptyArraysMustBelongToTheExactFramePathAndParent() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of()));
        try (var members = members(parent, List.of("other")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThatThrownBy(() -> read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
        var nested = observation(List.of(new ValidationRoot.Ancestor(List.of("parents"), key(11))), 1);
        try (var members = members(nested, List.of("items")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, nested, boundary("source"));
            assertThatThrownBy(() -> read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test void unknownKeyRulesAndOpenParentReadsCannotBecomeOrdinaryDataFindings() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of(Map.of("id", new ValidationValues.Typed("unadmitted", null, "codec", 11)))));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThatThrownBy(() -> read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
        var raw = side(Map.of("items", List.of()));
        var open = new ValidationCheckEvent.Side(raw.identity(), raw.row(), 1, ValidationCheckEvent.GroupStatus.OPEN, raw.boundary());
        try (var members = members(parent, List.of("items")); var root = builder()) {
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("source"));
            assertThatThrownBy(() -> read(parent, List.of("items"), open, open.row(), Optional.of(KEY), LIMITS, members, root))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test void malformedTargetCannotEraseACompleteIndependentSourceDuplicate() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of("items", List.of(Map.of("value", "missing-key"))));
        try (var members = members(parent, List.of("items")); var root = builder()) {
            source(members, parent, 11, "A"); source(members, parent, 11, "B");
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("children"));
            assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                    .isEqualTo(ValidationNestTargetArray.State.INVALID);
            root.read(Side.SOURCE, boundary("source"));
            var result = finish(root);
            assertThat(result.counts()).containsEntry(Kind.SOURCE_DUPLICATE, 1L).containsEntry(Kind.TARGET_KEY_INVALID, 1L);
            assertThat(result.findings().keySet()).anySatisfy(finding -> {
                assertThat(finding.kind()).isEqualTo(Kind.SOURCE_DUPLICATE);
                assertThat(finding.multiplicity()).contains(new Multiplicity(2, true));
                assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)));
            });
            assertThat(result.reads()).anySatisfy(read -> assertThat(read.boundary().readId()).isEqualTo("children"));
            assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.BLOCKED).containsEntry(Check.VALUES, Status.BLOCKED);
            assertThat(result.samples()).anySatisfy(sample -> assertThat(sample.source()).hasValueSatisfying(source -> assertThat(source.readId()).isEqualTo("children")));
        }
    }

    @Test void missingOrScalarTargetRetainsCrossElementSourceRowConflictsWithoutInferringMissingTargets() {
        var parent = observation(List.of(new ValidationRoot.Ancestor(List.of("parents"), key(5))), 1);
        for (var fields : List.of(Map.<String, Object>of(), Map.<String, Object>of("items", "wrong"))) {
            var doc = side(fields);
            try (var members = members(parent, List.of("items")); var root = builder()) {
                for (int element : List.of(21, 22)) members.add(ValidationNestArrayMembers.Side.SOURCE, parent,
                        new ValidationNestArrayMembers.Member(key(11), key(element),
                                new ValidationPhysicalIdentity("source", "db", null, "children", List.of(List.of("id")), key(11)),
                                ValidationValues.ownRow(Map.of("id", 11, "element", element), ROW)));
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("children"));
                assertThat(read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                        .isEqualTo(ValidationNestTargetArray.State.INVALID);
                var result = finish(root);
                assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_ROW_DUPLICATE, 2L, Kind.TARGET_STRUCTURE, 1L));
                assertThat(result.notCompared()).hasSize(4).allSatisfy(blocked -> {
                    assertThat(blocked.location().scope().ancestors()).hasSize(2);
                    assertThat(blocked.location().scope().ancestors().getFirst()).isEqualTo(new ValidationRoot.Ancestor(List.of("parents"), key(5)));
                });
            }
        }
    }

    @Test void anUnfinishedSourceCannotBeHiddenByAnEmptyOrMalformedTarget() {
        var parent = observation(List.of(), 1);
        for (var doc : List.of(side(Map.of("items", List.of())), side(Map.of()))) {
            try (var members = members(parent, List.of("items")); var root = builder()) {
                source(members, parent, 11, "A");
                assertThatThrownBy(() -> read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                        .isInstanceOfSatisfying(TapstateException.class, error -> {
                            assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT);
                            assertThat(error.args()).containsEntry("field", "nestMembers.sourceIncomplete");
                        });
                assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(members::finish).isInstanceOf(TapstateException.class);
            }
        }
    }

    @Test void sourceProblemsSurviveSampleExhaustionAndHardFindingLimitsStillAbort() {
        var parent = observation(List.of(), 1);
        var doc = side(Map.of());
        for (int findings : List.of(1, 2)) {
            try (var members = members(parent, List.of("items")); var root = ValidationRootCheck.builder(parent, ValidationMethod.FULL_COLUMN,
                    ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new ValidationRootCheck.Limits(1024 * 1024, findings, 100, 20, 0))) {
                source(members, parent, 11, "A"); source(members, parent, 11, "B");
                members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, boundary("children"));
                if (findings == 1) {
                    assertThatThrownBy(() -> read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root))
                            .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
                    assertThatThrownBy(root::finish).isInstanceOf(TapstateException.class);
                } else {
                    read(parent, List.of("items"), doc, doc.row(), Optional.of(KEY), LIMITS, members, root);
                    var result = finish(root);
                    assertThat(result.counts()).containsEntry(Kind.SOURCE_DUPLICATE, 1L).containsEntry(Kind.TARGET_STRUCTURE, 1L);
                    assertThat(result.samples()).isEmpty();
                    assertThat(result.samplesTruncated()).isTrue();
                }
            }
        }
    }

    private static ValidationNestTargetArray.State read(ValidationObservation parent, List<String> path, ValidationCheckEvent.Side physical,
            ValidationValues.Row working, Optional<ValidationKeyBinding> rowKey, ValidationNestTargetArray.Limits limits,
            ValidationNestArrayMembers members, Builder root) {
        return ValidationNestTargetArray.read(parent, path, physical, working, KEY, rowKey, Set.of("array", "json", "map"), limits, members, root);
    }
    private static ValidationNestArrayMembers members(ValidationObservation parent, List<String> path) {
        return new ValidationNestArrayMembers(parent, path, new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW));
    }
    private static void source(ValidationNestArrayMembers members, ValidationObservation parent, int id, String value) {
        members.add(ValidationNestArrayMembers.Side.SOURCE, parent, new ValidationNestArrayMembers.Member(key(id), key(id),
                new ValidationPhysicalIdentity("source", "db", null, "children", List.of(List.of("id")), key(id)),
                ValidationValues.ownRow(Map.of("id", id, "value", value), ROW)));
    }
    private static Builder builder() {
        return ValidationRootCheck.builder(observation(List.of(), 1), ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 10));
    }
    private static ValidationRootCheck finish(Builder root) {
        for (Check check : Check.values()) root.checked(check);
        return root.finish();
    }
    private static ValidationCheckEvent.Side side(Map<String, ?> fields) {
        return new ValidationCheckEvent.Side(new ValidationPhysicalIdentity("target", "db", null, "roots", List.of(List.of("id")), key(102)),
                ValidationValues.ownRow(fields, ROW), 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
    }
    private static Map<String, Object> nullable(String field) { var map = new HashMap<String, Object>(); map.put(field, null); return map; }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH); }
    private static ValidationObservation observation(List<ValidationRoot.Ancestor> ancestors, long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), ancestors), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
