package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;

class ValidationNestArraySourceReadTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final ValidationNestArraySourceRead.Limits LIMITS = limits(100, 20);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    private static final ValidationObservation PARENT = new ValidationObservation(new ValidationRunIdentity(
            new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0), "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
            new ValidationRoot("mapping", "output", key(7), List.of(new ValidationRoot.Ancestor(List.of("parents"), key(8)))), 1);
    private static final ValidationObservation ROOT = new ValidationObservation(PARENT.run(),
            new ValidationRoot("mapping", "output", key(7), List.of()), PARENT.revision());
    private static final ValidationKeyBinding ID = keys("id");
    private static final ValidationKeyBinding ELEMENT = keys("element");

    @Test void everyIncludedRowReachesMembershipAndTheRealTargetComposition() {
        var cursor = new Cursor(List.of(1, 2, 3, 4, 5, 6)); var facts = new AtomicInteger();
        var read = read(cursor, LIMITS, ValidationNestArraySourceReadTest::included, (context, source) -> {
            assertThat(context.parent()).isEqualTo(PARENT); assertThat(context.path()).containsExactly("items");
            assertThat(context.readId()).isEqualTo("children"); facts.incrementAndGet();
        }, () -> {});
        assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(6); assertThat(cursor.calls).isEqualTo(7);
        assertThat(facts).hasValue(6); assertThat(read.uncertain()).isEmpty();
        var source = read.source().orElseThrow(); assertThat(source.groups()).hasSize(6);
        var targetItems = new ArrayList<Map<String, Object>>();
        for (int id = 6; id >= 1; id--) targetItems.add(value(id));
        var expected = row(Map.of("label", "expected")); var targetRow = row(Map.of("label", "wrong", "items", targetItems));
        var physical = new ValidationPhysicalIdentity("target", "db", null, "roots", List.of(List.of("id")), key(7));
        var target = new ValidationCheckEvent.Side(physical, targetRow, 1, ValidationCheckEvent.GroupStatus.COMPLETE, boundary("target"));
        try (var members = ValidationNestArrayMembers.forTarget(source); var root = ValidationRootCheck.builder(ROOT, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, TargetPresence.PRESENT, new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 10))) {
            var checked = ValidationNestArrayFieldCheck.check(PARENT, List.of("items"), expected, target, targetRow, ELEMENT, Optional.of(ID), Set.of(),
                    new ValidationNestTargetArray.Limits(ROW, 100, 10000), members, root, group -> {
                        assertThat(group.scope().ancestors()).hasSize(2);
                        var a = group.sourceSamples().getFirst(); var b = group.targetSamples().getFirst();
                        ValidationFieldCheck.check(new ValidationObservation(PARENT.run(), group.scope(), PARENT.revision()), List.of(), a.values(), b.values(),
                                Optional.of(new SampledSide(a.physical(), a.original(), "children")), Optional.of(new SampledSide(b.physical(), b.original(), "target")),
                                Optional.empty(), String::equals, new ValidationValueComparison.TraversalLimits(ROW, 10000), root);
                    });
            ValidationFieldCheck.checkComposed(PARENT, expected, targetRow, Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                    new ValidationValueComparison.TraversalLimits(ROW, 10000), List.of(), Set.of(), List.of(checked), root);
            for (Check check : Check.values()) root.checked(check);
            assertThat(root.finish().counts()).containsExactly(entry(Kind.TARGET_VALUE, 1L));
        }
    }

    @Test void excludedAndIndeterminateRowsPreserveKnownLowerBoundsAndSourceEvidence() {
        var cursor = new Cursor(List.of(1, 2, 3, 4, 5, 5)); var recorded = new ArrayList<ValidationRootReader.Source>();
        var result = read(cursor, LIMITS, (context, raw) -> {
            int id = ((Number) raw.fields().get("id")).intValue();
            if (id == 2) return other(raw, ValidationRelationRows.Disposition.EXCLUDED, List.of());
            if (id == 4) return other(raw, ValidationRelationRows.Disposition.INDETERMINATE, List.of(cause("missing")));
            return included(context, raw);
        }, (context, source) -> recorded.add(source), () -> {});
        assertThat(result.source()).isEmpty(); assertThat(cursor.released).isEqualTo(6); assertThat(cursor.closed).isTrue();
        assertThat(recorded).hasSize(6);
        var uncertain = result.uncertain().orElseThrow();
        assertThat(uncertain.read().readId()).isEqualTo("children"); assertThat(uncertain.causes()).containsExactly(cause("missing"));
        try (var root = ValidationRootCheck.builder(ROOT, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 10))) {
            ValidationNestArrayCheck.checkUncertainSource(uncertain, root);
            root.read(Side.TARGET, boundary("target"));
            for (Check check : Check.values()) root.checked(check);
            var report = root.finish();
            assertThat(report.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_MISSING, 1L, Kind.SOURCE_DUPLICATE, 1L));
            assertThat(report.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.SOURCE_DUPLICATE)
                    .singleElement().satisfies(finding -> assertThat(finding.multiplicity()).contains(new Multiplicity(2, false)));
        }
    }

    @Test void noReferenceMissingAndUnresolvedNeverOpenOrInventCompleteEmptyMembership() {
        var nullable = new HashMap<String, Object>(); nullable.put("customer", null);
        var views = List.of(view(nullable, List.of()), view(Map.of(), List.of()), view(Map.of("customer", 7), List.of(List.of("customer"))));
        var kinds = List.of(ValidationNestLookupBinding.NoReference.class, ValidationNestLookupBinding.Invalid.class, ValidationNestLookupBinding.Unresolved.class);
        for (int i = 0; i < views.size(); i++) {
            var result = ValidationNestArraySourceRead.read(PARENT, binding(true), views.get(i), parentSample(), request -> { throw new AssertionError("No query"); },
                    LIMITS, CLOCK, () -> {}, ValidationNestArraySourceReadTest::included, (context, source) -> fail("No rows"));
            assertThat(result.preparation()).isInstanceOf(kinds.get(i)); assertThat(result.source()).isEmpty(); assertThat(result.uncertain()).isEmpty();
        }
        var empty = new Cursor(List.of());
        var result = read(empty, LIMITS, ValidationNestArraySourceReadTest::included, (context, source) -> {}, () -> {});
        assertThat(result.source().orElseThrow().groups()).isEmpty();
        assertThat(result.source().orElseThrow().read().readId()).isEqualTo("children"); assertThat(empty.calls).isEqualTo(1);
    }

    @Test void latePhysicalPredicateErrorsAndBudgetsCloseEveryBorrowedRow() {
        var predicate = new Cursor(List.of(1, 2, 3, 4)); predicate.badPredicateAt = 3;
        assertThatThrownBy(() -> read(predicate, LIMITS, ValidationNestArraySourceReadTest::included, (context, source) -> {}, () -> {}))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
        assertThat(predicate.released).isEqualTo(4); assertThat(predicate.closed).isTrue();
        var limited = new Cursor(List.of(1, 2, 3, 4));
        assertThatThrownBy(() -> read(limited, limits(3, 20), ValidationNestArraySourceReadTest::included, (context, source) -> {}, () -> {}))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "arrayRead.rows"));
        assertThat(limited.released).isEqualTo(4); assertThat(limited.closed).isTrue();
        var causes = new Cursor(List.of(1, 2, 3));
        assertThatThrownBy(() -> read(causes, limits(10, 1), (context, raw) -> other(raw, ValidationRelationRows.Disposition.INDETERMINATE,
                List.of(cause("field" + raw.fields().get("id")))), (context, source) -> {}, () -> {}))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.args()).containsEntry("limit", "arrayRead.causes"));
        assertThat(causes.released).isEqualTo(2); assertThat(causes.closed).isTrue();
    }

    @Test void incompleteEofEvidenceRetentionFailureAndCloseFailureNeverYieldSourceFacts() {
        var failure = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "children"), null);
        for (int scenario = 0; scenario < 3; scenario++) {
            var cursor = new Cursor(List.of(1, 2, 3));
            if (scenario == 0) cursor.eofFailure = failure;
            if (scenario == 1) cursor.closeFailure = failure;
            int current = scenario;
            assertThat(catchThrowable(() -> read(cursor, LIMITS, ValidationNestArraySourceReadTest::included, (context, source) -> {
                if (current == 2) throw failure;
            }, () -> {}))).isSameAs(failure);
            assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(scenario == 2 ? 1 : 3);
        }
    }

    @Test void authorityLossAtPhysicalCloseCannotPublishCompletedMembership() {
        var active = new AtomicBoolean(true); var cursor = new Cursor(List.of(1, 2, 3)); cursor.onClose = () -> active.set(false);
        var failure = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
        assertThat(catchThrowable(() -> read(cursor, LIMITS, ValidationNestArraySourceReadTest::included, (context, source) -> {},
                () -> { if (!active.get()) throw failure; }))).isSameAs(failure);
        assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(3);
    }

    @Test void aMemberCannotReplaceItsActualRawOrWorkingFacts() {
        for (boolean replaceRaw : List.of(false, true)) {
            var cursor = new Cursor(List.of(1));
            assertThatThrownBy(() -> read(cursor, LIMITS, (context, raw) -> {
                var original = included(context, raw); var member = original.member().orElseThrow();
                return new ValidationNestArraySourceRead.Classified(original.source(), Optional.of(new ValidationNestArrayMembers.Member(
                        member.rowKey(), member.elementKey(), member.physical(), replaceRaw ? row(raw.fields()) : raw,
                        replaceRaw ? member.values() : row(member.values().fields()))));
            }, (context, source) -> {}, () -> {})).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.args()).containsEntry("field", "arrayRead.memberFacts"));
            assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(1);
        }
    }

    @Test void nonNullReferenceMissingOrMultipleRowsCannotBecomeOrdinaryArrayMembership() {
        for (var ids : List.of(List.<Integer>of(), List.of(1, 2))) {
            var cursor = new Cursor(ids);
            var result = ValidationNestArraySourceRead.read(PARENT, binding(true), view(Map.of("customer", 7), List.of()), parentSample(), request ->
                    new ValidationRootReader.Query("children", "v1", () -> cursor), LIMITS, CLOCK, () -> {},
                    ValidationNestArraySourceReadTest::included, (context, source) -> {});
            assertThat(result.source()).isEmpty(); assertThat(result.uncertain()).isEmpty();
            var problem = result.referenceProblem().orElseThrow();
            assertThat(problem.state()).isEqualTo(ValidationNestSingleSource.State.BLOCKED);
            assertThat(problem.issues()).singleElement().satisfies(issue -> assertThat(issue.finding().kind())
                    .isEqualTo(ids.isEmpty() ? Kind.SOURCE_REFERENCE_MISSING : Kind.SOURCE_DUPLICATE));
            assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(ids.size());
        }
    }

    @Test void referenceParentBudgetIsIndependentOfTheSmallerChildRowBudget() {
        var parentLimits = new ValidationValues.Limits(65536, 100, 16);
        var parentRow = ValidationValues.ownRow(Map.of("id", 7, "customer", 7, "unrelated", "x".repeat(12000)), parentLimits);
        var view = ValidationStaticMap.compile("parent", new TransformBody.MapProjection(Map.of()), parentLimits).apply(parentRow, List.of());
        var original = new SampledSide(parentSample().identity(), parentRow, "parent-read");
        var cursor = new Cursor(List.of(1));
        var result = ValidationNestArraySourceRead.read(PARENT, binding(true, parentLimits), view, original, request ->
                new ValidationRootReader.Query("children", "v1", () -> cursor), LIMITS, CLOCK, () -> {},
                ValidationNestArraySourceReadTest::included, (context, source) -> {});
        assertThat(result.source().orElseThrow().groups()).hasSize(1);
        assertThat(result.referenceProblem()).isEmpty(); assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(1);
    }

    @Test void objectPrefixChangesOnlyLogicalScopeWhileQueriesStillBindLocalParentColumns() {
        var scope = new ValidationNestScope(PARENT).object(List.of("profile"));
        var cursor = new Cursor(List.of(1, 2, 3));
        var result = ValidationNestArraySourceRead.read(scope, binding(false), view(Map.of("id", 7), List.of()), parentSample(), request -> {
            assertThat(request.key()).isEqualTo(key(7));
            assertThat(request.physical().components().getFirst().path()).containsExactly("native_parent_id");
            return new ValidationRootReader.Query("profile-children", "v1", () -> cursor);
        }, LIMITS, CLOCK, () -> {}, (context, raw) -> {
            assertThat(context.path()).containsExactly("profile", "items");
            assertThat(context.parent()).isEqualTo(PARENT);
            return included(context, raw);
        }, (context, source) -> {});
        var facts = result.source().orElseThrow();
        assertThat(facts.arrayPath()).containsExactly("profile", "items");
        assertThat(facts.groups()).allSatisfy(group -> {
            assertThat(group.scope().rootKey()).isEqualTo(PARENT.root().rootKey());
            assertThat(group.scope().ancestors().getFirst()).isEqualTo(PARENT.root().ancestors().getFirst());
            assertThat(group.scope().ancestors().getLast().fieldPath()).containsExactly("profile", "items");
        });
        assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(3);
    }

    @Test void siblingArrayFramesShareRowQuotaAndAFailureCannotResetTheLedger() {
        var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 3);
        var first = new Cursor(List.of(1, 2));
        var source = sharedRead(new ValidationNestScope(PARENT).object(List.of("profile")), first, "first", budget,
                ValidationNestArraySourceReadTest::included).source().orElseThrow();
        assertThat(source.groups()).hasSize(2);
        var second = new Cursor(List.of(3, 4)); var classified = new AtomicInteger();
        Throwable failure = catchThrowable(() -> sharedRead(new ValidationNestScope(PARENT).object(List.of("billing")), second, "second", budget,
                (context, raw) -> { classified.incrementAndGet(); return included(context, raw); }));
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "tree.rows"));
        assertThat(classified).hasValue(1); assertThat(second.released).isEqualTo(2); assertThat(second.closed).isTrue();
        assertThat(catchThrowable(budget::reads)).isSameAs(failure);
        assertThat(catchThrowable(() -> budget.targetMembers(source))).isSameAs(failure);
        var third = new Cursor(List.of(5));
        assertThat(catchThrowable(() -> sharedRead(new ValidationNestScope(PARENT), third, "third", budget, ValidationNestArraySourceReadTest::included))).isSameAs(failure);
        assertThat(third.opens).isZero();
    }

    @Test void aLocalArrayFailureAlsoClosesTheSharedRootAndAllBorrowedRows() {
        var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 100);
        var cursor = new Cursor(List.of(1, 2));
        var failure = catchThrowable(() -> ValidationNestArraySourceRead.read(new ValidationNestScope(PARENT), binding(false),
                view(Map.of("id", 7), List.of()), parentSample(), request -> new ValidationRootReader.Query("child", "v1", () -> cursor),
                new ValidationNestArraySourceRead.Limits(LIMITS.read(), LIMITS.members(), 1, LIMITS.causes()),
                CLOCK, () -> {}, ValidationNestArraySourceReadTest::included, (context, source) -> {}, budget));
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "arrayRead.rows"));
        assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(2);
        assertThat(catchThrowable(budget::reads)).isSameAs(failure);
        var later = new Cursor(List.of(3));
        assertThat(catchThrowable(() -> sharedRead(new ValidationNestScope(PARENT), later, "later", budget,
                ValidationNestArraySourceReadTest::included))).isSameAs(failure);
        assertThat(later.opens).isZero();
    }

    @Test void sharedDepthRejectsANestedArrayBeforePreparingOrOpeningItsRead() {
        var budget = new ValidationNestBudget(ROOT, "parent-read", new ValidationNestBudget.Limits(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 1), "tree");
        var cursor = new Cursor(List.of(1)); var prepared = new AtomicInteger();
        assertThatThrownBy(() -> ValidationNestArraySourceRead.read(new ValidationNestScope(PARENT), binding(false), view(Map.of("id", 7), List.of()),
                parentSample(), request -> { prepared.incrementAndGet(); return new ValidationRootReader.Query("child", "v1", () -> { cursor.opens++; return cursor; }); },
                LIMITS, CLOCK, () -> {}, ValidationNestArraySourceReadTest::included, (context, source) -> {}, budget))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "tree.depth"));
        assertThat(prepared).hasValue(0); assertThat(cursor.opens).isZero();
    }

    @Test void targetLocalConstructionAndMutationFailuresInvalidateTheSharedRoot() {
        for (boolean construction : List.of(true, false)) {
            var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 100);
            var cursor = new Cursor(construction ? List.of() : List.of(1));
            var local = new ValidationNestArraySourceRead.Limits(LIMITS.read(),
                    new ValidationNestArrayMembers.Limits(construction ? 32 * 1024 : 1024 * 1024, 1, 10, ROW), 100, 20);
            var source = ValidationNestArraySourceRead.read(new ValidationNestScope(PARENT), binding(false), view(Map.of("id", 7), List.of()),
                    parentSample(), request -> new ValidationRootReader.Query("child", "v1", () -> cursor), local, CLOCK, () -> {},
                    ValidationNestArraySourceReadTest::included, (context, facts) -> {}, budget).source().orElseThrow();
            Throwable failure;
            if (construction) failure = catchThrowable(() -> budget.targetMembers(source));
            else {
                try (var target = budget.targetMembers(source)) {
                    var member = included(new ValidationNestArraySourceRead.Context(PARENT, List.of("items"), "target"), row(value(2))).member().orElseThrow();
                    failure = catchThrowable(() -> target.add(ValidationNestArrayMembers.Side.TARGET, PARENT, member));
                }
            }
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.args()).containsEntry("limit", construction ? "nestMembers.bytes" : "nestMembers.elementKeys"));
            assertThat(catchThrowable(budget::reads)).isSameAs(failure);
            var later = new Cursor(List.of(3));
            assertThat(catchThrowable(() -> sharedRead(new ValidationNestScope(PARENT), later, "later", budget,
                    ValidationNestArraySourceReadTest::included))).isSameAs(failure);
            assertThat(later.opens).isZero();
        }
    }

    @Test void frameBytesAndWorkAreReservedBeforeQueryPreparationOrOpening() {
        for (boolean bytes : List.of(true, false)) {
            var budget = budget(bytes ? 100 : 4 * 1024 * 1024, bytes ? 64 * 1024 * 1024 : 1, 8, 100);
            var prepares = new AtomicInteger(); var cursor = new Cursor(List.of(1));
            assertThatThrownBy(() -> ValidationNestArraySourceRead.read(new ValidationNestScope(PARENT), binding(false), view(Map.of("id", 7), List.of()),
                    parentSample(), request -> { prepares.incrementAndGet(); return new ValidationRootReader.Query("child", "v1", () -> { cursor.opens++; return cursor; }); },
                    LIMITS, CLOCK, () -> {}, ValidationNestArraySourceReadTest::included, (context, source) -> {}, budget))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", bytes ? "tree.bytes" : "tree.operations"));
            assertThat(prepares).hasValue(0); assertThat(cursor.opens).isZero();
        }
    }

    @Test void recursiveReadsAccountForTheirStillLiveOuterRowAndCloseBothCursorsOnExhaustion() {
        var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 2);
        var outer = new Cursor(List.of(1)); var inner = new Cursor(List.of(2, 3)); inner.relationKey = 1;
        var visited = new AtomicInteger();
        assertThatThrownBy(() -> sharedRead(new ValidationNestScope(PARENT), outer, "outer", budget, (context, raw) -> {
            var projected = included(context, raw); var member = projected.member().orElseThrow();
            var child = new ValidationNestScope(PARENT).member(List.of("items"), member.elementKey());
            ValidationNestArraySourceRead.read(child, binding(false), view(projected.source().row().working().fields(), List.of()),
                    new SampledSide(member.physical(), raw, context.readId()), request -> new ValidationRootReader.Query("inner", "v1", () -> { inner.opens++; return inner; }),
                    LIMITS, CLOCK, () -> {}, (nested, value) -> { visited.incrementAndGet(); return included(nested, value); }, (nested, source) -> {}, budget);
            return projected;
        })).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("limit", "tree.rows"));
        assertThat(visited).hasValue(1); assertThat(outer.released).isEqualTo(1); assertThat(inner.released).isEqualTo(2);
        assertThat(outer.closed).isTrue(); assertThat(inner.closed).isTrue();
    }

    @Test void queryIdsAndQueryLimitsRemainGlobalAcrossSiblingFrames() {
        for (boolean duplicate : List.of(true, false)) {
            var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, duplicate ? 8 : 1, 100);
            sharedRead(new ValidationNestScope(PARENT), new Cursor(List.of(1)), "first", budget, ValidationNestArraySourceReadTest::included);
            var next = new Cursor(List.of(2));
            assertThatThrownBy(() -> sharedRead(new ValidationNestScope(PARENT).object(List.of("billing")), next, duplicate ? "first" : "second", budget,
                    ValidationNestArraySourceReadTest::included)).isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.args()).containsEntry(duplicate ? "field" : "limit", duplicate ? "tree.reusedReadId" : "tree.queries"));
            assertThat(next.opens).isZero();
        }
    }

    @Test void targetIndexCoexistenceUsesTheSameRootReservationAndRequiresTheActualCompletedRead() {
        long capacity = 4 * 1024 * 1024;
        var budget = budget(capacity, 64 * 1024 * 1024, 8, 100);
        var source = sharedRead(new ValidationNestScope(PARENT), new Cursor(List.of(1, 2)), "source", budget,
                ValidationNestArraySourceReadTest::included).source().orElseThrow();
        long initial = ValidationNestArrayMembers.initialReservationBytes(source.observation(), source.arrayPath());
        budget.bytes(capacity - budget.accountedBytes() - initial);
        assertThatThrownBy(() -> budget.targetMembers(source)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "tree.bytes"));
        assertThat(source.groups()).hasSize(2);
        var unrelated = budget(capacity, 64 * 1024 * 1024, 8, 100);
        assertThatThrownBy(() -> unrelated.targetMembers(source)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("field", "tree.sourceReadIncomplete"));
    }

    @Test void foreignObservationsAndUnregisteredParentReadsCannotSpendTheRootBudget() {
        for (boolean foreign : List.of(true, false)) {
            var budget = budget(4 * 1024 * 1024, 64 * 1024 * 1024, 8, 100);
            var scope = new ValidationNestScope(foreign ? new ValidationObservation(PARENT.run(), PARENT.root(), PARENT.revision() + 1) : PARENT);
            var sample = foreign ? parentSample() : new SampledSide(parentSample().identity(), parentSample().row(), "unknown-parent");
            var cursor = new Cursor(List.of(1));
            assertThatThrownBy(() -> ValidationNestArraySourceRead.read(scope, binding(false), view(Map.of("id", 7), List.of()), sample,
                    request -> new ValidationRootReader.Query("child", "v1", () -> { cursor.opens++; return cursor; }), LIMITS, CLOCK, () -> {},
                    ValidationNestArraySourceReadTest::included, (context, source) -> {}, budget)).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.args()).containsEntry("field", foreign ? "tree.scope" : "tree.parentRead"));
            assertThat(cursor.opens).isZero();
        }
    }

    private static ValidationNestBudget budget(long bytes, long operations, int queries, int rows) {
        return new ValidationNestBudget(ROOT, "parent-read", new ValidationNestBudget.Limits(bytes, operations, queries, rows, 16), "tree");
    }
    private static ValidationNestArraySourceRead.Result sharedRead(ValidationNestScope scope, Cursor cursor, String id, ValidationNestBudget budget,
            ValidationNestArraySourceRead.Rule rule) {
        return ValidationNestArraySourceRead.read(scope, binding(false), view(Map.of("id", 7), List.of()), parentSample(), request ->
                new ValidationRootReader.Query(id, "v1", () -> { cursor.opens++; return cursor; }), LIMITS, CLOCK, () -> {}, rule, (context, source) -> {}, budget);
    }

    private static ValidationNestArraySourceRead.Result read(Cursor cursor, ValidationNestArraySourceRead.Limits limits,
            ValidationNestArraySourceRead.Rule rule, ValidationNestArraySourceRead.Facts facts, Runnable guard) {
        return ValidationNestArraySourceRead.read(PARENT, binding(false), view(Map.of("id", 7), List.of()), parentSample(), request ->
                new ValidationRootReader.Query("children", "v1", () -> cursor), limits, CLOCK, guard, rule, facts);
    }
    private static ValidationNestArraySourceRead.Classified included(ValidationNestArraySourceRead.Context context, ValidationValues.Row raw) {
        var rowKey = ((ValidationKeyBinding.Bound) ID.read(raw)).key(); var element = ((ValidationKeyBinding.Bound) ELEMENT.read(raw)).key();
        var working = row(value(((Number) raw.fields().get("id")).intValue()));
        var physical = new ValidationPhysicalIdentity("source", "db", null, "children", List.of(List.of("id")), rowKey);
        var source = new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                working, Optional.of(physical)), List.of());
        return new ValidationNestArraySourceRead.Classified(source, Optional.of(new ValidationNestArrayMembers.Member(Optional.of(rowKey), element, physical, raw, working)));
    }
    private static ValidationNestArraySourceRead.Classified other(ValidationValues.Row raw, ValidationRelationRows.Disposition disposition, List<Finding> causes) {
        return new ValidationNestArraySourceRead.Classified(new ValidationRootReader.Source(
                new ValidationRelationReader.Evaluation(disposition, raw, Optional.empty()), causes), Optional.empty());
    }
    private static SampledSide parentSample() {
        return new SampledSide(new ValidationPhysicalIdentity("source", "db", null, "parents", List.of(List.of("id")), key(7)),
                row(Map.of("id", 7, "customer", 7)), "parent-read");
    }
    private static Finding cause(String name) { return new Finding(Kind.SOURCE_FIELD_MISSING, new Location(PARENT.root(), List.of("items", name))); }
    private static ValidationNestLookupBinding binding(boolean reference) { return binding(reference, ROW); }
    private static ValidationNestLookupBinding binding(boolean reference, ValidationValues.Limits parentLimits) {
        String child = reference ? "id" : "parent_id"; String parent = reference ? "customer" : "id";
        var embed = new Embed("children", Map.of(child, parent), EmbedAs.ARRAY, "items", List.of("id"), List.of("element"), null, null, List.of());
        var plan = ValidationNestRelations.compile("nest", new NestRoot("root", List.of("id"), null, null, List.of(embed)),
                Set.of("root", "children"), Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
        return new ValidationNestLookupBinding("nest", plan.children().getFirst(), List.of(component(parent)), keys("native_parent_id"), parentLimits);
    }
    private static ValidationStaticMap.Projection view(Map<String, ?> fields, List<List<Object>> unknown) {
        return ValidationStaticMap.compile("parent", new TransformBody.MapProjection(Map.of()), ROW).apply(row(fields), unknown);
    }
    private static ValidationNestArraySourceRead.Limits limits(int rows, int causes) {
        return new ValidationNestArraySourceRead.Limits(new ValidationRelationRows.Limits(65536, ROW),
                new ValidationNestArrayMembers.Limits(1024 * 1024, 100, 100, ROW), rows, causes);
    }
    private static ValidationKeyBinding keys(String field) { return new ValidationKeyBinding(field, List.of(component(field)), Set.of(), ROW); }
    private static ValidationKeyBinding.Component component(String field) { return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")); }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static Map<String, Object> value(int id) { return Map.of("id", id, "element", id + 100, "value", "value-" + id); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationCheckEvent.ReadBoundary boundary(String id) { return new ValidationCheckEvent.ReadBoundary(id, "v1", Instant.EPOCH, Instant.EPOCH); }
    private static final class Cursor implements ValidationRowCursor {
        final List<Integer> ids; int delivered; int released; int calls; int opens; int relationKey = 7; boolean closed; int badPredicateAt = -1;
        RuntimeException eofFailure; RuntimeException closeFailure; Runnable onClose = () -> {};
        Cursor(List<Integer> ids) { this.ids = ids; }
        @Override public Optional<Row> next() {
            calls++;
            if (closed || released != delivered) throw new IllegalStateException("unreleased row");
            if (delivered == ids.size()) { if (eofFailure != null) throw eofFailure; return Optional.empty(); }
            int ordinal = delivered++; int id = ids.get(ordinal);
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return row(Map.of("id", id, "element", id + 100, "native_parent_id", ordinal == badPredicateAt ? 999 : relationKey)); }
                @Override public void close() { released++; }
            });
        }
        @Override public void close() { closed = true; onClose.run(); if (closeFailure != null) throw closeFailure; }
    }
}
