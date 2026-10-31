package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationNestArrayMembers.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestArraySourceFactsTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(16384, 1024, 16);
    private static final Limits LIMITS = new Limits(1024 * 1024, 100, 100, ROW);
    private static final ValidationObservation PARENT = new ValidationObservation(new ValidationRunIdentity(
            new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0), "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
            new ValidationRoot("mapping", "output", key(102), List.of(new ValidationRoot.Ancestor(List.of("parents"), key(8)))), 1);

    @Test void allMembersAndPhysicalIdentitiesSurviveSourceHandoffBeforeTargetExists() {
        var expected = new ArrayList<Member>();
        for (int i = 1; i <= 6; i++) expected.add(member(10 + i, i, "value-" + i));
        var source = facts(expected, LIMITS);
        assertThat(source.groups()).hasSize(6);
        assertThat(source.read().readId()).isEqualTo("source");
        assertThat(source.observation()).isEqualTo(PARENT);
        assertThat(source.arrayPath()).containsExactly("items");
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            for (int i = expected.size() - 1; i >= 0; i--) target.add(Side.TARGET, PARENT, expected.get(i));
            target.complete(Side.TARGET, PARENT, boundary("target"));
            var result = target.finish();
            assertThat(result.groups()).hasSize(6).allMatch(Group::canCompareValues);
            assertThat(result.groups()).allSatisfy(group -> {
                assertThat(group.scope().ancestors().getFirst()).isEqualTo(PARENT.root().ancestors().getFirst());
                assertThat(group.sourceSamples().getFirst().physical().actualKey()).isEqualTo(
                        key(((Number) group.sourceSamples().getFirst().values().fields().get("id")).longValue() + 1000));
            });
            assertThat(result.reads()).containsOnlyKeys(Side.SOURCE, Side.TARGET);
        }
        assertThat(source.groups()).hasSize(6).allSatisfy(group -> assertThat(group.samples()).hasSize(1));
    }

    @Test void exactMultiplicityAndLateRowIdentityCollisionsSurviveBeyondTwoSamples() {
        var rows = new ArrayList<Member>();
        for (int i = 0; i < 20; i++) rows.add(member(10 + i, 1, "duplicate-" + i));
        rows.add(member(29, 2, "late-row-collision"));
        var source = facts(rows, LIMITS);
        assertThat(source.groups()).hasSize(2).allMatch(SourceGroup::conflict);
        assertThat(source.groups().getFirst().count()).isEqualTo(20);
        assertThat(source.groups().getFirst().samples()).hasSize(2);
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            target.add(Side.TARGET, PARENT, member(10, 1, "one"));
            target.add(Side.TARGET, PARENT, member(29, 2, "two"));
            target.complete(Side.TARGET, PARENT, boundary("target"));
            var result = target.finish();
            assertThat(result.groups()).allMatch(Group::sourceConflict).noneMatch(Group::canCompareValues);
            assertThat(result.groups().getFirst().sourceCount()).isEqualTo(20);
        }
    }

    @Test void openSourceOrAnyTargetActivityCannotBeFrozenAsSourceOnlyFacts() {
        for (int state = 0; state < 3; state++) {
            try (var members = new ValidationNestArrayMembers(PARENT, List.of("items"), LIMITS)) {
                if (state > 0) members.complete(Side.SOURCE, PARENT, boundary("source"));
                if (state == 1) members.add(Side.TARGET, PARENT, member(1, 1, "A"));
                if (state == 2) members.complete(Side.TARGET, PARENT, boundary("target"));
                assertThatThrownBy(members::finishSource).isInstanceOf(TapstateException.class);
                assertThatThrownBy(members::finish).isInstanceOf(TapstateException.class);
            }
        }
    }

    @Test void emptySourceRemainsIncompleteForTheTargetAndLateSourceRowsAreRejected() {
        var source = facts(List.of(), LIMITS);
        assertThat(source.groups()).isEmpty();
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            assertThatThrownBy(target::finish).isInstanceOf(TapstateException.class);
        }
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            assertThatThrownBy(() -> target.add(Side.SOURCE, PARENT, member(1, 1, "late"))).isInstanceOf(TapstateException.class);
        }
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            target.complete(Side.TARGET, PARENT, boundary("target"));
            assertThat(target.finish().groups()).isEmpty();
        }
    }

    @Test void ancestorRevisionAndKeyDomainsRemainBindingAfterHandoff() {
        var source = facts(List.of(member(11, 1, "A")), LIMITS);
        for (var wrong : List.of(new ValidationObservation(PARENT.run(), PARENT.root(), 2),
                new ValidationObservation(PARENT.run(), new ValidationRoot("mapping", "output", key(102), List.of()), 1))) {
            try (var target = ValidationNestArrayMembers.forTarget(source)) {
                assertThatThrownBy(() -> target.add(Side.TARGET, wrong, member(11, 1, "A"))).isInstanceOf(TapstateException.class);
            }
        }
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            var ordinary = member(11, 1, "A");
            var changed = new Member(ordinary.rowKey(), new ValidationKey(List.of(ValidationKey.Part.text("1"))), ordinary.physical(), ordinary.values());
            assertThatThrownBy(() -> target.add(Side.TARGET, PARENT, changed)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.INCOMPATIBLE_KEY));
        }
    }

    @Test void originalKeyAndCoexistingMetadataBudgetsCannotBeRaisedAtTargetHandoff() {
        var source = facts(List.of(member(11, 1, "A")), new Limits(1024 * 1024, 1, 1, ROW));
        try (var target = ValidationNestArrayMembers.forTarget(source)) {
            assertThatThrownBy(() -> target.add(Side.TARGET, PARENT, member(12, 2, "B"))).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        }
        var narrow = facts(List.of(), new Limits(20000, 1, 1, ROW));
        assertThatThrownBy(() -> ValidationNestArrayMembers.forTarget(narrow)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    private static Source facts(List<Member> values, Limits limits) {
        try (var source = new ValidationNestArrayMembers(PARENT, List.of("items"), limits)) {
            for (var value : values) source.add(Side.SOURCE, PARENT, value);
            source.complete(Side.SOURCE, PARENT, boundary("source"));
            var result = source.finishSource();
            assertThatThrownBy(source::finish).isInstanceOf(IllegalStateException.class);
            return result;
        }
    }
    private static Member member(long id, long element, String text) {
        var row = ValidationValues.ownRow(Map.of("id", id, "element", element, "value", text), ROW);
        return new Member(key(id), key(element), new ValidationPhysicalIdentity("source", "db", null, "children",
                List.of(List.of("physical_id")), key(id + 1000)), row);
    }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH); }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
