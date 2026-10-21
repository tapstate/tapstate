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
    @Test void transformedMembersKeepWorkingValuesForComparisonAndRawRowsForDiagnostics() {
        var observation = observation(102, 1);
        var original = member(11, 1, "raw");
        var working = ValidationValues.ownRow(Map.of("renamed", "checked"), ROW);
        var source = new ValidationNestArrayMembers.Member(original.rowKey(), original.elementKey(), original.physical(), original.values(), working);
        var missing = members(observation, List.of(source), List.of());
        var builder = builder(observation, ValidationMethod.FULL_COLUMN);
        ValidationNestArrayCheck.check(missing, builder, ignored -> { throw new AssertionError("No target exists"); });
        assertThat(finish(builder).samples()).singleElement().satisfies(sample -> {
            assertThat(sample.source().orElseThrow().row()).isSameAs(original.values());
            assertThat(sample.source().orElseThrow().identity()).isEqualTo(original.physical());
            assertThat(sample.source().orElseThrow().row().fields()).containsEntry("value", "raw").doesNotContainKey("renamed");
        });
        var target = new ValidationNestArrayMembers.Member(original.rowKey(), original.elementKey(), original.physical(), working);
        var complete = members(observation, List.of(source), List.of(target));
        var matched = builder(observation, ValidationMethod.FULL_COLUMN);
        ValidationNestArrayCheck.check(complete, matched, group -> {
            assertThat(group.sourceSamples().getFirst().values()).isSameAs(working);
            assertThat(ValidationValueComparison.compare(group.sourceSamples().getFirst().values(), group.targetSamples().getFirst().values())).isEmpty();
        });
        assertThat(finish(matched).observedAgreement()).isTrue();
    }

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

    @Test void uncertainSourceMembershipBlocksTheWholeArrayWithoutInventingTargetDifferences() {
        var observation = observation(102, 1);
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation.root(), List.of("eligible")));
        for (boolean hasKnownMember : List.of(false, true)) {
            try (var staged = sourceFrame(observation)) {
                if (hasKnownMember) staged.add(ValidationNestArrayMembers.Side.SOURCE, observation, member(11, 1, "known"));
                staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
                var uncertain = staged.finishUncertainSource(List.of(cause));
                assertThatThrownBy(staged::finish).isInstanceOf(IllegalStateException.class);
                var root = builder(observation, ValidationMethod.FULL_COLUMN).read(Side.TARGET, read("target-root"));
                ValidationNestArrayCheck.checkUncertainSource(uncertain, root);
                var result = finish(root);
                assertThat(result.counts()).containsOnly(entry(Kind.SOURCE_FIELD_MISSING, 1L));
                assertThat(result.notCompared()).hasSize(2).allSatisfy(blocked ->
                        assertThat(blocked.location()).isEqualTo(new Location(observation.root(), List.of("items"))));
                assertThat(result.observedAgreement()).isFalse();
                assertThat(result.reads()).noneMatch(boundary -> boundary.boundary().readId().equals("target-array"));
            }
        }
    }

    @Test void unknownArrayParticipationCannotMakeAParentEmptyArrayPredicateTrue() {
        var observation = observation(102, 1);
        var empty = ValidationPredicate.compile("empty", new ValidationPredicate.Compare(ValidationPredicate.Operator.EQ,
                new ValidationPredicate.ArraySize(new ValidationPredicate.Field(List.of("items"))), new ValidationPredicate.Constant(0)),
                new ValidationPredicate.Limits(ROW, 1000, false));
        try (var staged = sourceFrame(observation)) {
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            var uncertain = staged.finishUncertainSource(List.of(new Finding(Kind.SOURCE_FIELD_MISSING,
                    new Location(observation.root(), List.of("eligible")))));
            var working = ValidationValues.ownRow(Map.of("id", 102), ROW);
            var evaluated = empty.evaluate(working, List.of(new ArrayList<Object>(uncertain.arrayPath())));
            assertThat(evaluated.matches()).isNull();
            assertThat(evaluated.problem().kind()).isEqualTo(ValidationPredicate.ProblemKind.UNAVAILABLE_FIELD);
            assertThat(empty.evaluate(ValidationValues.ownRow(Map.of("items", List.of()), ROW)).matches()).isTrue();
        }
    }

    @Test void alreadyProvenSourceDuplicatesSurviveUnknownMembershipAsLowerBounds() {
        var observation = observation(102, 1);
        var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(observation.root(), List.of("eligible")));
        try (var staged = sourceFrame(observation)) {
            staged.add(ValidationNestArrayMembers.Side.SOURCE, observation, member(11, 1, "raw-A"));
            staged.add(ValidationNestArrayMembers.Side.SOURCE, observation, member(12, 1, "raw-B"));
            staged.add(ValidationNestArrayMembers.Side.SOURCE, observation, member(13, 2, "uncontested"));
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            var root = builder(observation, ValidationMethod.FULL_COLUMN).read(Side.TARGET, read("target-root"));
            ValidationNestArrayCheck.checkUncertainSource(staged.finishUncertainSource(List.of(cause)), root);
            var result = finish(root);
            assertThat(result.counts()).containsOnly(entry(Kind.SOURCE_FIELD_INVALID, 1L), entry(Kind.SOURCE_DUPLICATE, 1L));
            assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.SOURCE_DUPLICATE)
                    .singleElement().satisfies(finding -> assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
            assertThat(result.samples()).singleElement().satisfies(sample ->
                    assertThat(sample.source().orElseThrow().row().fields()).containsEntry("value", "raw-A"));
        }
    }

    @Test void uncertainSourceRequiresNormalEofASourceCauseAndNoInventedTargetFacts() {
        var observation = observation(102, 1);
        var cause = new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation.root(), List.of("eligible")));
        try (var staged = sourceFrame(observation)) {
            assertThatThrownBy(() -> staged.finishUncertainSource(List.of(cause))).isInstanceOf(TapstateException.class);
        }
        for (var causes : List.of(List.<Finding>of(), List.of(new Finding(Kind.TARGET_VALUE, cause.location())),
                List.of(new Finding(Kind.SOURCE_FIELD_MISSING, new Location(observation(999, 1).root(), List.of("eligible")))))) {
            try (var staged = sourceFrame(observation)) {
                staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
                assertThatThrownBy(() -> staged.finishUncertainSource(causes)).isInstanceOf(TapstateException.class);
            }
        }
        try (var staged = sourceFrame(observation)) {
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            staged.complete(ValidationNestArrayMembers.Side.TARGET, observation, read("target-array"));
            assertThatThrownBy(() -> staged.finishUncertainSource(List.of(cause))).isInstanceOf(TapstateException.class);
        }
    }

    @Test void uncertainSourceFactsKeepTheOriginalObservationFence() {
        var observation = observation(102, 1);
        try (var staged = sourceFrame(observation)) {
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            var uncertain = staged.finishUncertainSource(List.of(new Finding(Kind.SOURCE_FIELD_INVALID,
                    new Location(observation.root(), List.of("eligible")))));
            var newer = builder(observation(102, 2), ValidationMethod.FULL_COLUMN);
            assertThatThrownBy(() -> ValidationNestArrayCheck.checkUncertainSource(uncertain, newer)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(newer::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void uncertainMembershipCannotBorrowProblemsFromASiblingArray() {
        var observation = observation(102, 1);
        var sibling = new ValidationRoot("mapping", "output", observation.root().rootKey(),
                List.of(new ValidationRoot.Ancestor(List.of("refunds"), key(9))));
        try (var staged = sourceFrame(observation)) {
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            assertThatThrownBy(() -> staged.finishUncertainSource(List.of(new Finding(Kind.SOURCE_FIELD_INVALID,
                    new Location(sibling, List.of("eligible")))))).isInstanceOf(TapstateException.class);
        }
        var descendant = new ValidationRoot("mapping", "output", observation.root().rootKey(), List.of(
                new ValidationRoot.Ancestor(List.of("items"), key(7)), new ValidationRoot.Ancestor(List.of("details"), key(2))));
        try (var staged = sourceFrame(observation)) {
            staged.complete(ValidationNestArrayMembers.Side.SOURCE, observation, read("source"));
            var cause = new Finding(Kind.SOURCE_FIELD_INVALID, new Location(descendant, List.of("eligible")));
            assertThat(staged.finishUncertainSource(List.of(cause)).causes()).containsExactly(cause);
        }
    }

    private static ValidationNestArrayMembers sourceFrame(ValidationObservation observation) {
        return new ValidationNestArrayMembers(observation, List.of("items"), new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW));
    }
    private static ValidationCheckEvent.ReadBoundary read(String id) {
        return new ValidationCheckEvent.ReadBoundary(id, "complete", Instant.EPOCH, Instant.EPOCH);
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
