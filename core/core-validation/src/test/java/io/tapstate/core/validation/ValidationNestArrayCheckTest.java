package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

/** Complete local fact composition, not native read completeness, a Jet job or strict confirmation. */
class ValidationNestArrayCheckTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final Limits ROOT = new Limits(1024 * 1024, 100, 100, 30, 1);

    @Test void aSourceConflictBlocksOnlyItsScopeAndIndependentPairsAndRootsContinue() {
        var root = observation(102, 1);
        var facts = members(root, List.of(member(11, 1, "A"), member(12, 1, "B"), member(13, 2, "C")),
                List.of(member(11, 1, "A"), member(13, 2, "changed")));
        var builder = builder(root, ValidationMethod.FULL_COLUMN);
        var visited = new ArrayList<ValidationKey>();
        ValidationNestArrayCheck.check(facts, builder, group -> {
            visited.add(group.scope().ancestors().getLast().key());
            values(facts, group, builder);
        });
        var result = finish(builder);
        assertThat(visited).containsExactly(key(2));
        assertThat(result.counts()).containsEntry(Kind.SOURCE_DUPLICATE, 1L).containsEntry(Kind.TARGET_VALUE, 1L);
        assertThat(result.counts()).doesNotContainKeys(Kind.TARGET_MISSING, Kind.TARGET_EXTRA);
        assertThat(result.checks()).containsEntry(Check.STRUCTURE, Status.BLOCKED).containsEntry(Check.VALUES, Status.BLOCKED);
        assertThat(result.notCompared()).hasSize(2).allSatisfy(blocked ->
                assertThat(blocked.location().scope().ancestors().getLast().key()).isEqualTo(key(1)));
        for (int rootKey : List.of(101, 103)) {
            var normal = observation(rootKey, 1);
            var normalBuilder = builder(normal, ValidationMethod.FULL_COLUMN);
            var normalFacts = members(normal, List.of(member(11, 1, "A")), List.of(member(11, 1, "A")));
            ValidationNestArrayCheck.check(normalFacts, normalBuilder, group -> values(normalFacts, group, normalBuilder));
            assertThat(finish(normalBuilder).observedAgreement()).isTrue();
        }
    }

    @Test void targetDuplicatesAndAbsentMembersRetainInapplicablePathsWithoutPickingWinners() {
        var root = observation(102, 1);
        var facts = members(root, List.of(member(11, 1, "A"), member(12, 2, "B")),
                List.of(member(11, 1, "A"), member(13, 1, "other"), member(14, 3, "extra")));
        var builder = builder(root, ValidationMethod.FULL_COLUMN);
        ValidationNestArrayCheck.check(facts, builder, group -> { throw new AssertionError("No unique matched member exists"); });
        var result = finish(builder);
        assertThat(result.counts()).containsEntry(Kind.TARGET_DUPLICATE, 1L).containsEntry(Kind.TARGET_MISSING, 1L).containsEntry(Kind.TARGET_EXTRA, 1L);
        assertThat(result.notCompared()).isEmpty();
        assertThat(result.notApplicableScopes()).hasSize(6);
        assertThat(result.notApplicableScopes()).extracting(Skipped::check).contains(Check.STRUCTURE, Check.VALUES);
        assertThat(result.samples()).hasSize(1);
        assertThat(result.samplesTruncated()).isTrue();
        assertThat(result.observedAgreement()).isFalse();
    }

    @Test void rowIdentityConflictsDifferFromLocalElementDuplicates() {
        var root = observation(102, 1);
        var facts = members(root, List.of(member(11, 1, "A"), member(11, 2, "B")),
                List.of(member(12, 1, "A"), member(12, 2, "B")));
        var builder = builder(root, ValidationMethod.FULL_COLUMN);
        ValidationNestArrayCheck.check(facts, builder, group -> { throw new AssertionError("Identity conflicts cannot select a winner"); });
        var result = finish(builder);
        assertThat(result.counts()).containsEntry(Kind.SOURCE_ROW_DUPLICATE, 2L).containsEntry(Kind.TARGET_ROW_DUPLICATE, 2L);
        assertThat(result.findings().keySet()).allSatisfy(finding -> assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
    }

    @Test void uniqueKeyChecksObservableIdentityButDoesNotCompareUnrelatedPayloads() {
        var root = observation(102, 1);
        var facts = members(root, List.of(member(11, 1, "A")), List.of(member(11, 1, "wrong payload")));
        var unique = builder(root, ValidationMethod.UNIQUE_KEY);
        ValidationNestArrayCheck.check(facts, unique, group -> assertThat(group.rowIdentityComparison())
                .isEqualTo(ValidationNestArrayMembers.RowIdentityComparison.MATCHES));
        assertThat(finish(unique).observedAgreement()).isTrue();
        var wrongIdentity = members(root, List.of(member(11, 1, "A")), List.of(member(99, 1, "A")));
        var invalid = builder(root, ValidationMethod.UNIQUE_KEY);
        ValidationNestArrayCheck.check(wrongIdentity, invalid, ignored -> {});
        assertThat(finish(invalid).counts()).containsEntry(Kind.TARGET_IDENTITY, 1L);
    }

    @Test void fixedHiddenTargetIdentityIsNotInventedOrMistakenForADataIssue() {
        var root = observation(102, 1);
        var source = member(11, 1, "A");
        var hidden = new ValidationNestArrayMembers.Member(Optional.empty(), source.elementKey(), source.physical(), source.values());
        var facts = members(root, List.of(source), List.of(hidden));
        var builder = builder(root, ValidationMethod.UNIQUE_KEY);
        var observed = new AtomicInteger();
        ValidationNestArrayCheck.check(facts, builder, group -> {
            assertThat(group.rowIdentityComparison()).isEqualTo(ValidationNestArrayMembers.RowIdentityComparison.NOT_OBSERVABLE);
            observed.incrementAndGet();
        });
        assertThat(observed).hasValue(1);
        assertThat(finish(builder).observedAgreement()).isTrue();
    }

    @Test void staleOrOtherRootFactsAbortStagingEvenWhenTheArrayIsEmpty() {
        var root = observation(102, 2);
        for (var wrong : List.of(observation(102, 1), observation(103, 2))) {
            var builder = builder(root, ValidationMethod.FULL_COLUMN);
            var facts = members(wrong, List.of(), List.of());
            assertThatThrownBy(() -> ValidationNestArrayCheck.check(facts, builder, ignored -> {})).isInstanceOf(TapstateException.class);
            assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void callbackAndValueBudgetFailuresCannotLeavePublishablePartialRootFacts() {
        var root = observation(102, 1);
        var facts = members(root, List.of(member(11, 1, "A")), List.of(member(11, 1, "B")));
        var builder = builder(root, ValidationMethod.FULL_COLUMN);
        var failure = new AssertionError("failed descendant");
        assertThat(catchThrowable(() -> ValidationNestArrayCheck.check(facts, builder, ignored -> { throw failure; }))).isSameAs(failure);
        assertThatThrownBy(builder::finish).isInstanceOf(IllegalStateException.class);
        var bounded = builder(root, ValidationMethod.FULL_COLUMN);
        assertThatThrownBy(() -> ValidationNestArrayCheck.check(facts, bounded, group ->
                ValidationValueComparison.visitDifferences(group.sourceSamples().getFirst().values(), group.targetSamples().getFirst().values(),
                        String::equals, new ValidationValueComparison.TraversalLimits(ROW, 1), ignored -> {})))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(bounded::finish).isInstanceOf(IllegalStateException.class);
    }

    @Test void unknownRootParticipationCannotGainTargetConclusionsThroughAChildArray() {
        var root = observation(102, 1);
        var builder = ValidationRootCheck.builder(root, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.UNKNOWN, TargetPresence.PRESENT, ROOT);
        var facts = members(root, List.of(), List.of(member(11, 1, "A")));
        assertThatThrownBy(() -> ValidationNestArrayCheck.check(facts, builder, ignored -> {})).isInstanceOf(TapstateException.class);
    }

    @Test void skippedScopesRequireActualStructuralCausesAndShareTheHardScopeBudget() {
        var root = observation(102, 1);
        var location = new Location(root.root(), List.of("field"));
        var builder = builder(root, ValidationMethod.FULL_COLUMN);
        var value = new Finding(Kind.TARGET_VALUE, location);
        builder.finding(value);
        assertThatThrownBy(() -> builder.notApplicable(Check.VALUES, location, value)).isInstanceOf(TapstateException.class);
        var narrow = ValidationRootCheck.builder(root, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new Limits(1024 * 1024, 10, 1, 10, 0));
        var missing = new Finding(Kind.TARGET_MISSING, location);
        narrow.finding(missing).notApplicable(Check.STRUCTURE, location, missing);
        assertThatThrownBy(() -> narrow.notApplicable(Check.VALUES, location, missing)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(narrow::finish).isInstanceOf(TapstateException.class);
    }

    private static void values(ValidationNestArrayMembers.Result facts, ValidationNestArrayMembers.Group group, Builder builder) {
        ValidationValueComparison.visitDifferences(group.sourceSamples().getFirst().values(), group.targetSamples().getFirst().values(),
                String::equals, new ValidationValueComparison.TraversalLimits(ROW, 10000), difference -> {
                    Kind kind = switch (difference.kind()) {
                        case VALUE -> Kind.TARGET_VALUE;
                        case LOGICAL_TYPE -> Kind.TARGET_TYPE;
                        case MISSING_SOURCE -> Kind.TARGET_EXTRA;
                        case MISSING_TARGET -> Kind.TARGET_MISSING;
                    };
                    var source = group.sourceSamples().getFirst();
                    var target = group.targetSamples().getFirst();
                    builder.finding(new Finding(kind, new Location(group.scope(), difference.path())),
                            Optional.of(new SampledSide(source.physical(), source.values(), facts.reads().get(ValidationNestArrayMembers.Side.SOURCE).readId())),
                            Optional.of(new SampledSide(target.physical(), target.values(), facts.reads().get(ValidationNestArrayMembers.Side.TARGET).readId())));
                });
    }
    private static Builder builder(ValidationObservation root, ValidationMethod method) {
        return ValidationRootCheck.builder(root, method, ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, ROOT);
    }
    private static ValidationRootCheck finish(Builder builder) {
        builder.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE);
        if (builder.method() == ValidationMethod.FULL_COLUMN) builder.checked(Check.VALUES);
        return builder.finish();
    }
    private static ValidationNestArrayMembers.Result members(ValidationObservation parent,
            List<ValidationNestArrayMembers.Member> source, List<ValidationNestArrayMembers.Member> target) {
        try (var members = new ValidationNestArrayMembers(parent, List.of("items"), new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW))) {
            source.forEach(member -> members.add(ValidationNestArrayMembers.Side.SOURCE, parent, member));
            target.forEach(member -> members.add(ValidationNestArrayMembers.Side.TARGET, parent, member));
            for (var side : ValidationNestArrayMembers.Side.values()) members.complete(side, parent,
                    new ValidationCheckEvent.ReadBoundary(side.name(), "complete", Instant.EPOCH, Instant.EPOCH));
            return members.finish();
        }
    }
    private static ValidationNestArrayMembers.Member member(int row, int element, String value) {
        return new ValidationNestArrayMembers.Member(key(row), key(element),
                new ValidationPhysicalIdentity("connection", "database", null, "table", List.of(List.of("physical")), key(1000 + row)),
                ValidationValues.ownRow(Map.of("value", value), ROW));
    }
    private static ValidationObservation observation(int root, long revision) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules");
        return new ValidationObservation(run, new ValidationRoot("mapping", "output", key(root), List.of()), revision);
    }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
}
