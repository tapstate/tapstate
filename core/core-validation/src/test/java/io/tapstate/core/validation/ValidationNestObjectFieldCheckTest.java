package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestObjectFieldCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(65536, 1000, 20);
    private static final ValidationValueComparison.TraversalLimits WALK = new ValidationValueComparison.TraversalLimits(ROW, 100_000);
    private static final ValidationObservation ROOT = new ValidationObservation(new ValidationRunIdentity(
            new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0), "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
            new ValidationRoot("mapping", "output", key(102), List.of()), 1);
    private static final ValidationKeyBinding KEY = new ValidationKeyBinding("id", List.of(new ValidationKeyBinding.Component(List.of("id"),
            ValidationKey.Kind.EXACT_NUMBER, Set.of())), Set.of(), ROW);

    @Test void objectWithUnorderedArrayIsCheckedOnceWhileOrdinaryParentFieldsStillCompare() {
        var source = row(Map.of("id", 102, "label", "expected"));
        var profile = row(Map.of("id", 7, "name", "Ada"));
        var target = side(row(Map.of("id", 102, "label", "wrong", "profile", Map.of("id", 7, "name", "Ada", "items",
                List.of(Map.of("id", 12, "amount", 4), Map.of("id", 11, "amount", 3))))));
        try (var root = builder()) {
            var checked = ValidationNestObjectFieldCheck.check(new ValidationNestScope(ROOT), List.of("profile"), source, object(profile),
                    target, target.row(), Set.of(), WALK, root, (left, right) -> {
                        var scope = new ValidationNestScope(ROOT).object(List.of("profile"));
                        try (var members = new ValidationNestArrayMembers(ROOT, List.of("profile", "items"),
                                new ValidationNestArrayMembers.Limits(1024 * 1024, 20, 20, ROW))) {
                            for (int i = 11; i <= 12; i++) members.add(ValidationNestArrayMembers.Side.SOURCE, ROOT,
                                    new ValidationNestArrayMembers.Member(key(i), key(i), identity("source", i), row(Map.of("id", i, "amount", i - 8))));
                            members.complete(ValidationNestArrayMembers.Side.SOURCE, ROOT, boundary("items"));
                            var array = ValidationNestArrayFieldCheck.check(scope, List.of("items"), left.working(), target, right, KEY, Optional.of(KEY),
                                    Set.of(), new ValidationNestTargetArray.Limits(ROW, 20, 100_000), members, root, group -> {
                                        var a = group.sourceSamples().getFirst(); var b = group.targetSamples().getFirst();
                                        ValidationFieldCheck.check(new ValidationObservation(ROOT.run(), group.scope(), ROOT.revision()), List.of(), a.values(), b.values(),
                                                Optional.empty(), Optional.empty(), Optional.empty(), String::equals, WALK, root);
                                    });
                            ValidationFieldCheck.checkComposed(scope, left.working(), right, Optional.empty(), Optional.empty(), Optional.empty(),
                                    String::equals, WALK, List.of(), Set.of(), List.of(array), root);
                        }
                    });
            ValidationFieldCheck.checkComposed(ROOT, source, target.row(), Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                    WALK, List.of(), Set.of(), List.of(checked), root);
            var result = finish(root);
            assertThat(result.findings().keySet()).singleElement().satisfies(finding -> {
                assertThat(finding.kind()).isEqualTo(Kind.TARGET_VALUE);
                assertThat(finding.location().fieldPath()).containsExactly("label");
            });
        }
    }

    @Test void missingOrMalformedObjectKeepsItsFindingAndSkipsOnlyItsOwnSubtree() {
        for (var fields : List.of(Map.<String, Object>of("id", 102, "label", "wrong"),
                Map.<String, Object>of("id", 102, "label", "wrong", "profile", "bad"))) {
            var source = row(Map.of("id", 102, "label", "expected")); var target = side(row(fields)); var callbacks = new AtomicInteger();
            try (var root = builder()) {
                var checked = ValidationNestObjectFieldCheck.check(new ValidationNestScope(ROOT), List.of("profile"), source,
                        object(row(Map.of("id", 7))), target, target.row(), Set.of(), WALK, root, (left, right) -> callbacks.incrementAndGet());
                ValidationFieldCheck.checkComposed(ROOT, source, target.row(), Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                        WALK, List.of(), Set.of(), List.of(checked), root);
                var result = finish(root);
                assertThat(callbacks).hasValue(0);
                assertThat(result.findings()).hasSize(2);
                assertThat(result.findings().keySet()).anySatisfy(finding -> assertThat(finding.location().fieldPath()).containsExactly("profile"));
                assertThat(result.findings().keySet()).anySatisfy(finding -> assertThat(finding.location().fieldPath()).containsExactly("label"));
            }
        }
    }

    @Test void proofsCannotBeReusedWithAnotherRowOrCoordinateAndCallbackFailureClosesTheRoot() {
        for (boolean wrongRow : List.of(true, false)) {
            var source = row(Map.of("id", 102)); var target = side(row(Map.of("id", 102, "profile", Map.of("id", 7))));
            try (var root = builder()) {
                var checked = ValidationNestObjectFieldCheck.check(new ValidationNestScope(ROOT), List.of("profile"), source,
                        object(row(Map.of("id", 7))), target, target.row(), Set.of(), WALK, root, (left, right) ->
                                ValidationFieldCheck.check(new ValidationNestScope(ROOT).object(List.of("profile")).observation(), List.of("profile"),
                                        left.working(), right, Optional.empty(), Optional.empty(), Optional.empty(), String::equals, WALK, root));
                var scope = wrongRow ? new ValidationNestScope(ROOT) : new ValidationNestScope(ROOT).object(List.of("other"));
                assertThatThrownBy(() -> ValidationFieldCheck.checkComposed(scope, wrongRow ? row(source.fields()) : source, target.row(),
                        Optional.empty(), Optional.empty(), Optional.empty(), String::equals, WALK, List.of(), Set.of(), List.of(checked), root))
                        .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("field", "fieldCheck.objectEvidenceScope"));
            }
        }
        try (var root = builder()) {
            var target = side(row(Map.of("profile", Map.of("id", 7)))); var failure = new IllegalStateException("broken checker");
            assertThatThrownBy(() -> ValidationNestObjectFieldCheck.check(new ValidationNestScope(ROOT), List.of("profile"), row(Map.of()),
                    object(row(Map.of("id", 7))), target, target.row(), Set.of(), WALK, root, (left, right) -> { throw failure; })).isSameAs(failure);
            assertThatThrownBy(root::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    private static ValidationNestSingleSource.Result object(ValidationValues.Row row) {
        try (var source = new ValidationRelationRows(ROOT, "profile", new ValidationRelationRows.Limits(1024 * 1024, ROW))) {
            source.accept(ROOT, "profile", ValidationRelationRows.Disposition.INCLUDED,
                    new ValidationRelationRows.Sample(row, row, Optional.of(identity("source", 7))));
            source.complete(ROOT, boundary("profile"));
            var location = new Location(ROOT.root(), List.of("profile"));
            return ValidationNestSingleSource.classify(source.finish(), ValidationNestSingleSource.Relation.GATHER, Optional.empty(), location,
                    List.of(location), List.of(), 1024 * 1024);
        }
    }
    private static Builder builder() {
        var root = ValidationRootCheck.builder(ROOT, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new Limits(2 * 1024 * 1024, 100, 100, 20, 10));
        root.read(Side.SOURCE, boundary("root")); root.read(Side.TARGET, boundary("target")); return root;
    }
    private static ValidationRootCheck finish(Builder root) { for (Check check : Check.values()) root.checked(check); return root.finish(); }
    private static ValidationCheckEvent.Side side(ValidationValues.Row row) { return new ValidationCheckEvent.Side(identity("target", 102), row, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target")); }
    private static ValidationPhysicalIdentity identity(String side, long id) { return new ValidationPhysicalIdentity(side, "db", null, "table", List.of(List.of("id")), key(id)); }
    private static ValidationKey key(long id) { return new ValidationKey(List.of(ValidationKey.Part.number(id))); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "v1", Instant.EPOCH, Instant.EPOCH); }
}
