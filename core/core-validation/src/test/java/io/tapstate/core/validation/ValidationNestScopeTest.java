package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

/** Scoped checker composition; query admission and whole-tree execution are separate obligations. */
class ValidationNestScopeTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(65536, 1000, 20);
    private static final ValidationObservation ROOT = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
            "task", "run", ValidationMode.FULL, "spec", 1, "rules"), new ValidationRoot("mapping", "output", key(102), List.of()), 1);
    private static final ValidationKeyBinding KEY = new ValidationKeyBinding("id", List.of(new ValidationKeyBinding.Component(
            List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), ROW);

    @Test void arrayObjectArrayCoordinatesKeepOriginalRootBothAncestorKeysAndObjectFieldPrefix() {
        var item = new ValidationNestScope(ROOT).member(List.of("items"), key(11));
        var profile = item.object(List.of("profile"));
        var expected = row(Map.of("id", 7, "name", "expected"));
        var actualProfile = Map.<String, Object>of("id", 7, "name", "wrong", "adjustments", List.of(Map.of("id", 501, "amount", 9)));
        var actualItem = row(Map.of("id", 11, "profile", actualProfile));
        var rawTarget = row(Map.of("id", 102, "items", List.of(actualItem.fields())));
        var physical = new ValidationCheckEvent.Side(identity("target", 102), rawTarget, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
        ValidationNestSingleSource.Result object;
        try (var rows = new ValidationRelationRows(item.observation(), "profile-source", new ValidationRelationRows.Limits(1024 * 1024, ROW))) {
            rows.accept(item.observation(), "profile-source", ValidationRelationRows.Disposition.INCLUDED,
                    new ValidationRelationRows.Sample(expected, expected, Optional.of(identity("source", 7))));
            rows.complete(item.observation(), boundary("profile-source"));
            object = ValidationNestSingleSource.classify(rows.finish(), ValidationNestSingleSource.Relation.GATHER, Optional.empty(),
                    item.location(List.of("profile")), List.of(item.location(List.of("profile"))), List.of(), 1024 * 1024);
        }
        try (var root = builder()) {
            ValidationNestObjectCheck.check(item, List.of("profile"), object, Optional.of(new ValidationNestObjectCheck.Target(physical, actualItem)),
                    Set.of(), ROW, root, (source, target) -> {
                        try (var members = frame(profile, List.of("adjustments"))) {
                            var child = row(Map.of("id", 501, "amount", 5));
                            members.add(ValidationNestArrayMembers.Side.SOURCE, item.observation(),
                                    new ValidationNestArrayMembers.Member(key(501), key(501), identity("source", 501), child));
                            members.complete(ValidationNestArrayMembers.Side.SOURCE, item.observation(), boundary("adjustments-source"));
                            var checked = ValidationNestArrayFieldCheck.check(profile, List.of("adjustments"), source.working(), physical, target, KEY, Optional.of(KEY),
                                    Set.of(), new ValidationNestTargetArray.Limits(ROW, 100, 10000), members, root, group -> {
                                        var a = group.sourceSamples().getFirst(); var b = group.targetSamples().getFirst();
                                        ValidationFieldCheck.check(new ValidationObservation(ROOT.run(), group.scope(), ROOT.revision()), List.of(), a.values(), b.values(),
                                                Optional.of(new SampledSide(a.physical(), a.original(), "adjustments-source")),
                                                Optional.of(new SampledSide(b.physical(), b.original(), "target")), Optional.empty(), String::equals,
                                                new ValidationValueComparison.TraversalLimits(ROW, 10000), root);
                                    });
                            ValidationFieldCheck.checkComposed(profile, source.working(), target, Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                                    new ValidationValueComparison.TraversalLimits(ROW, 10000), List.of(), Set.of(), List.of(checked), root);
                        }
                    });
            var result = finish(root);
            assertThat(result.counts()).containsExactly(entry(Kind.TARGET_VALUE, 2L));
            assertThat(result.findings().keySet()).anySatisfy(finding -> {
                assertThat(finding.location().scope().rootKey()).isEqualTo(key(102));
                assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)),
                        new ValidationRoot.Ancestor(List.of("profile", "adjustments"), key(501)));
                assertThat(finding.location().fieldPath()).containsExactly("amount");
            }).anySatisfy(finding -> {
                assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)));
                assertThat(finding.location().fieldPath()).containsExactly("profile", "name");
            });
        }
    }

    @Test void localArrayLookupDoesNotSearchTheObjectPrefixAgainAndBadContainersKeepFullPaths() {
        var scope = new ValidationNestScope(ROOT).member(List.of("items"), key(11)).object(List.of("profile"));
        for (var target : List.of(row(Map.of("adjustments", List.of())), row(Map.of()))) {
            var physical = new ValidationCheckEvent.Side(identity("target", 102), target, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
            try (var frame = frame(scope, List.of("adjustments")); var root = builder()) {
                frame.complete(ValidationNestArrayMembers.Side.SOURCE, scope.observation(), boundary("source"));
                var source = row(Map.of());
                var checked = ValidationNestArrayFieldCheck.check(scope, List.of("adjustments"), source, physical, target, KEY, Optional.of(KEY), Set.of(),
                        new ValidationNestTargetArray.Limits(ROW, 100, 10000), frame, root, group -> fail("Empty membership"));
                ValidationFieldCheck.checkComposed(scope, source, target, Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                        new ValidationValueComparison.TraversalLimits(ROW, 10000), List.of(), Set.of(), List.of(checked), root);
                var result = finish(root);
                if (target.fields().containsKey("adjustments")) assertThat(result.observedAgreement()).isTrue();
                else assertThat(result.findings().keySet()).singleElement().satisfies(finding -> {
                    assertThat(finding.kind()).isEqualTo(Kind.TARGET_STRUCTURE);
                    assertThat(finding.location().fieldPath()).containsExactly("profile", "adjustments");
                    assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"), key(11)));
                });
            }
        }
    }

    @Test void equalLocalKeysUnderSiblingObjectsDoNotShareMembershipOrComparisonEvidence() {
        var parent = new ValidationNestScope(ROOT).member(List.of("items"), key(11));
        var left = parent.object(List.of("profile")); var right = parent.object(List.of("billing"));
        assertThat(left.member(List.of("adjustments"), key(501)).observation().root())
                .isNotEqualTo(right.member(List.of("adjustments"), key(501)).observation().root());
        assertThat(left.member(List.of("adjustments"), key(501)).observation().root().rootKey()).isEqualTo(ROOT.root().rootKey());
        assertThat(left.member(List.of("adjustments"), key(501)).objectPath()).isEmpty();
        var source = row(Map.of()); var target = row(Map.of("adjustments", List.of()));
        var physical = new ValidationCheckEvent.Side(identity("target", 102), target, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
        try (var frame = frame(left, List.of("adjustments")); var root = builder()) {
            frame.complete(ValidationNestArrayMembers.Side.SOURCE, left.observation(), boundary("source"));
            var checked = ValidationNestArrayFieldCheck.check(left, List.of("adjustments"), source, physical, target, KEY, Optional.of(KEY), Set.of(),
                    new ValidationNestTargetArray.Limits(ROW, 100, 10000), frame, root, group -> fail("Empty array"));
            assertThatThrownBy(() -> ValidationFieldCheck.checkComposed(right, source, target, Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                    new ValidationValueComparison.TraversalLimits(ROW, 10000), List.of(), Set.of(), List.of(checked), root))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("field", "fieldCheck.arrayEvidenceScope"));
        }
    }

    @Test void fullPathDepthAndFrameScopeAreCheckedBeforeContainerTraversal() {
        var prefix = java.util.Collections.nCopies(32, "object");
        var full = new ValidationNestScope(ROOT, prefix);
        assertThatThrownBy(() -> full.path(List.of("items"))).isInstanceOf(TapstateException.class);
        var scope = new ValidationNestScope(ROOT).object(List.of("profile"));
        var target = row(Map.of("items", List.of()));
        var physical = new ValidationCheckEvent.Side(identity("target", 102), target, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
        try (var frame = frame(new ValidationNestScope(ROOT), List.of("items")); var root = builder()) {
            frame.complete(ValidationNestArrayMembers.Side.SOURCE, ROOT, boundary("source"));
            assertThatThrownBy(() -> ValidationNestTargetArray.read(scope, List.of("items"), physical, target, KEY, Optional.of(KEY), Set.of(),
                    new ValidationNestTargetArray.Limits(ROW, 100, 10000), frame, root)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    private static ValidationNestArrayMembers frame(ValidationNestScope scope, List<String> path) {
        return new ValidationNestArrayMembers(scope.observation(), scope.path(path), new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW));
    }
    private static Builder builder() { return ValidationRootCheck.builder(ROOT, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
            TargetPresence.PRESENT, new ValidationRootCheck.Limits(2 * 1024 * 1024, 100, 100, 20, 10)); }
    private static ValidationRootCheck finish(Builder root) { for (Check check : Check.values()) root.checked(check); return root.finish(); }
    private static ValidationPhysicalIdentity identity(String side, long id) { return new ValidationPhysicalIdentity(side, "db", null, "rows", List.of(List.of("id")), key(id)); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "v1", Instant.EPOCH, Instant.EPOCH); }
}
