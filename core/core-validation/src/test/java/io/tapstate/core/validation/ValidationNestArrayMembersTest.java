package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationNestArrayMembers.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class ValidationNestArrayMembersTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(16384, 1024, 16);
    private static final Limits LIMITS = new Limits(1024 * 1024, 100, 100, ROW);

    @Test void arbitraryMemberOrderMatchesWithoutSortingAndKeepsSeparateRowAndArrayKeys() {
        var parent = observation(List.of(), 1);
        var source = List.of(member(11, 1, "first"), member(12, 2, "second"));
        var target = List.of(member(12, 2, "second"), member(11, 1, "first"));
        var result = members(parent, List.of("lines"), source, target);
        assertThat(result.groups()).hasSize(2).allMatch(Group::canCompareValues);
        assertThat(group(result, 1).scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("lines"), key(1)));
        for (Group group : result.groups()) {
            assertThat(ValidationValueComparison.compare(group.sourceSamples().getFirst().values(),
                    group.targetSamples().getFirst().values())).isEmpty();
        }
        assertThat(group(result, 1).sourceSamples().getFirst().rowKey()).contains(key(11));
        assertThat(group(result, 1).sourceSamples().getFirst().physical().actualKey()).isEqualTo(key(1011));
    }

    @Test void multiLevelAndSiblingArraysKeepEveryAncestorEvenWhenLocalKeysRepeat() {
        var root = observation(List.of(), 1);
        var lines = members(root, List.of("lines"), List.of(member(11, 1, "A"), member(12, 2, "B")),
                List.of(member(12, 2, "B"), member(11, 1, "A")));
        var scopes = new ArrayList<ValidationRoot>();
        for (Group line : lines.groups()) {
            var parent = new ValidationObservation(root.run(), line.scope(), root.revision());
            var splits = members(parent, List.of("splits"), List.of(member(501, 1, "X"), member(502, 2, "Y")),
                    List.of(member(502, 2, "Y"), member(501, 1, "X")));
            var sibling = members(parent, List.of("discounts"), List.of(member(501, 1, "Z")), List.of(member(501, 1, "Z")));
            assertThat(splits.groups()).allMatch(Group::canCompareValues);
            scopes.addAll(splits.groups().stream().map(Group::scope).toList());
            scopes.add(sibling.groups().getFirst().scope());
        }
        assertThat(scopes).hasSize(6).doesNotHaveDuplicates();
        assertThat(scopes).allSatisfy(scope -> assertThat(scope.ancestors()).hasSize(2));
        assertThat(scopes).contains(new ValidationRoot("mapping", "output", key(102), List.of(
                new ValidationRoot.Ancestor(List.of("lines"), key(1)), new ValidationRoot.Ancestor(List.of("splits"), key(1)))));
    }

    @Test void missingExtraWrongParentAndWrongValueAreIndependentFacts() {
        var root = observation(List.of(), 1);
        var parentOne = new ValidationObservation(root.run(), scoped(root, "lines", 1), 1);
        var parentTwo = new ValidationObservation(root.run(), scoped(root, "lines", 2), 1);
        var oldParent = members(parentOne, List.of("splits"), List.of(member(501, 1, "A")), List.of());
        var newParent = members(parentTwo, List.of("splits"), List.of(), List.of(member(501, 1, "A")));
        assertThat(oldParent.groups().getFirst().sourceCount()).isEqualTo(1);
        assertThat(oldParent.groups().getFirst().targetCount()).isZero();
        assertThat(newParent.groups().getFirst().sourceCount()).isZero();
        assertThat(newParent.groups().getFirst().targetCount()).isEqualTo(1);
        assertThat(oldParent.groups().getFirst().scope()).isNotEqualTo(newParent.groups().getFirst().scope());
        var badValue = members(parentOne, List.of("splits"), List.of(member(501, 1, "A")), List.of(member(501, 1, "wrong")));
        var group = badValue.groups().getFirst();
        assertThat(group.canCompareValues()).isTrue();
        assertThat(ValidationValueComparison.compare(group.sourceSamples().getFirst().values(), group.targetSamples().getFirst().values()))
                .get().extracting(ValidationValueComparison.Difference::path).isEqualTo(List.of("value"));
    }

    @Test void duplicateMembersAreCountedNotOverwrittenAndOnlyTwoSamplesAreRetained() {
        var parent = observation(List.of(), 1);
        try (var index = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS)) {
            for (int i = 0; i < 50; i++) index.add(Side.SOURCE, parent, member(11, 1, "source-" + i));
            for (int i = 0; i < 70; i++) index.add(Side.TARGET, parent, member(11, 1, "target-" + i));
            index.add(Side.SOURCE, parent, member(12, 2, "normal"));
            index.add(Side.TARGET, parent, member(12, 2, "normal"));
            complete(index, parent);
            var result = index.finish();
            var duplicate = result.groups().getFirst();
            assertThat(duplicate.sourceCount()).isEqualTo(50);
            assertThat(duplicate.targetCount()).isEqualTo(70);
            assertThat(duplicate.sourceSamples()).hasSize(2);
            assertThat(duplicate.targetSamples()).hasSize(2);
            assertThat(duplicate.sourceSamples().getFirst().values().fields()).containsEntry("value", "source-0");
            assertThat(duplicate.sourceConflict()).isTrue();
            assertThat(duplicate.targetConflict()).isTrue();
            assertThat(duplicate.canCompareValues()).isFalse();
            assertThat(result.groups().get(1).canCompareValues()).isTrue();
        }
    }

    @Test void sameDeclaredRowIdentityUnderDifferentElementKeysBlocksBothCompetitors() {
        var parent = observation(List.of(), 1);
        var result = members(parent, List.of("items"), List.of(member(11, 1, "A"), member(11, 2, "B")),
                List.of(member(11, 1, "A"), member(12, 2, "B")));
        assertThat(result.groups()).allMatch(Group::sourceConflict).noneMatch(Group::canCompareValues);
        assertThat(result.groups()).noneMatch(Group::targetConflict);
        var targetConflict = members(parent, List.of("items"), List.of(member(11, 1, "A"), member(12, 2, "B")),
                List.of(member(11, 1, "A"), member(11, 2, "B")));
        assertThat(targetConflict.groups()).allMatch(Group::targetConflict);
    }

    @Test void emptyCompleteArraysAreValidButOneCompletedSideIsNotEnough() {
        var parent = observation(List.of(), 1);
        assertThat(members(parent, List.of("items"), List.of(), List.of()).groups()).isEmpty();
        var partial = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        partial.complete(Side.SOURCE, parent, boundary("s"));
        Throwable failure = catchThrowable(partial::finish);
        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(catchThrowable(partial::finish)).isSameAs(failure);
        var closed = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        closed.close();
        assertThatThrownBy(closed::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void oldRevisionAndOtherParentCallbacksCannotMixIntoThisArray() {
        var parent = observation(List.of(), 2);
        for (var wrong : List.of(observation(List.of(), 1), observation(List.of(new ValidationRoot.Ancestor(List.of("lines"), key(1))), 2))) {
            var index = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
            assertThatThrownBy(() -> index.add(Side.SOURCE, wrong, member(1, 1, "A"))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(index::finish).isInstanceOf(TapstateException.class);
        }
    }

    @Test void completedSidesRejectLateRowsAndChangedBoundaries() {
        var parent = observation(List.of(), 1);
        var index = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        index.complete(Side.SOURCE, parent, boundary("s"));
        index.complete(Side.SOURCE, parent, boundary("s"));
        assertThatThrownBy(() -> index.add(Side.SOURCE, parent, member(1, 1, "A"))).isInstanceOf(TapstateException.class);
        var other = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        other.complete(Side.SOURCE, parent, boundary("s"));
        assertThatThrownBy(() -> other.complete(Side.SOURCE, parent, boundary("different"))).isInstanceOf(TapstateException.class);
    }

    @Test void keysAndMemoryStayBoundedIncludingDistinctRowIdentitiesInsideADuplicateBucket() {
        var parent = observation(List.of(), 1);
        var elementLimit = new ValidationNestArrayMembers(parent, List.of("items"), new Limits(1024 * 1024, 1, 10, ROW));
        elementLimit.add(Side.SOURCE, parent, member(1, 1, "A"));
        Throwable failure = catchThrowable(() -> elementLimit.add(Side.SOURCE, parent, member(2, 2, "B")));
        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(catchThrowable(elementLimit::finish)).isSameAs(failure);
        var rowLimit = new ValidationNestArrayMembers(parent, List.of("items"), new Limits(1024 * 1024, 10, 1, ROW));
        rowLimit.add(Side.SOURCE, parent, member(1, 1, "A"));
        assertThatThrownBy(() -> rowLimit.add(Side.SOURCE, parent, member(2, 1, "B"))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationNestArrayMembers(parent, List.of("items"), new Limits(100, 1, 1, ROW)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void incompatibleDomainsCannotBecomeUnrelatedMissingAndExtraMembers() {
        var parent = observation(List.of(), 1);
        var index = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        index.add(Side.SOURCE, parent, member(1, 1, "A"));
        var normal = member(1, 1, "A");
        var textKey = new Member(normal.rowKey(), new ValidationKey(List.of(ValidationKey.Part.text("1"))), normal.physical(), normal.values());
        assertThatThrownBy(() -> index.add(Side.TARGET, parent, textKey)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.INCOMPATIBLE_KEY));
    }

    @Test void hiddenTargetRowIdentityIsExplicitAndCannotPretendToBeVerified() {
        var parent = observation(List.of(), 1);
        var source = member(11, 1, "A");
        var hidden = new Member(Optional.empty(), source.elementKey(), source.physical(), source.values());
        var result = members(parent, List.of("items"), List.of(source), List.of(hidden));
        assertThat(result.groups().getFirst().canCompareValues()).isTrue();
        assertThat(result.groups().getFirst().rowIdentityComparison()).isEqualTo(RowIdentityComparison.NOT_OBSERVABLE);
        var changed = members(parent, List.of("items"), List.of(source), List.of(member(99, 1, "A")));
        assertThat(changed.groups().getFirst().rowIdentityComparison()).isEqualTo(RowIdentityComparison.DIFFERS);
        var equal = members(parent, List.of("items"), List.of(source), List.of(source));
        assertThat(equal.groups().getFirst().rowIdentityComparison()).isEqualTo(RowIdentityComparison.MATCHES);
        var invalid = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        assertThatThrownBy(() -> invalid.add(Side.SOURCE, parent, hidden)).isInstanceOf(TapstateException.class);
        var mixed = new ValidationNestArrayMembers(parent, List.of("items"), LIMITS);
        mixed.add(Side.TARGET, parent, hidden);
        assertThatThrownBy(() -> mixed.add(Side.TARGET, parent, source)).isInstanceOf(TapstateException.class);
    }

    private static Group group(Result result, int elementKey) {
        return result.groups().stream().filter(group -> group.scope().ancestors().getLast().key().equals(key(elementKey)))
                .findFirst().orElseThrow();
    }
    private static Result members(ValidationObservation parent, List<String> path, List<Member> source, List<Member> target) {
        try (var index = new ValidationNestArrayMembers(parent, path, LIMITS)) {
            for (Member member : target) index.add(Side.TARGET, parent, member);
            for (Member member : source) index.add(Side.SOURCE, parent, member);
            complete(index, parent);
            return index.finish();
        }
    }
    private static void complete(ValidationNestArrayMembers index, ValidationObservation observation) {
        index.complete(Side.SOURCE, observation, boundary("source"));
        index.complete(Side.TARGET, observation, boundary("target"));
    }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) {
        return new ValidationCheckEvent.ReadBoundary(id, "complete-query-v1", Instant.EPOCH, Instant.EPOCH);
    }
    private static Member member(int row, int element, String value) {
        return new Member(key(row), key(element), new ValidationPhysicalIdentity("source", "db", null, "table",
                List.of(List.of("physical_id")), key(1000 + row)),
                ValidationValues.ownRow(Map.of("id", row, "local", element, "value", value), ROW));
    }
    private static ValidationRoot scoped(ValidationObservation parent, String path, int key) {
        return new ValidationRoot("mapping", "output", key(102), List.of(new ValidationRoot.Ancestor(List.of(path), key(key))));
    }
    private static ValidationObservation observation(List<ValidationRoot.Ancestor> ancestors, long revision) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules");
        return new ValidationObservation(run, new ValidationRoot("mapping", "output", key(102), ancestors), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
